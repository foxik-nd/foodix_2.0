import os
import logging
import json
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Depends, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from hdfs import InsecureClient
import httpx
import pandas as pd
import pickle
from scipy import sparse
from kafka import KafkaProducer
import json as pyjson
from fastapi import APIRouter
import glob

# ────── CONFIGURATION ENV ──────
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "hdfs")
HDFS_USERS_PATH = "/users/users.json"
HDFS_PRODUCTS_PATH = "/products/products.json"
HDFS_LOGS_PATH = "/logs/user_events.json"
hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "produits_scannes")

SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

# ────── INIT APP ──────
app = FastAPI()
producer = None

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ────── VECTORIZER ET DONNÉES RECOMMANDATION ──────
with open('./model/vectorizer.pkl', 'rb') as f:
    vectorizer = pickle.load(f)
green_df = pd.read_csv('./model/green_df.csv')
green_vectors = sparse.load_npz('./model/green_vectors.npz')

# ────── UTILS HDFS: USERS ──────
def read_users_from_hdfs():
    try:
        with hdfs_client.read(HDFS_USERS_PATH) as reader:
            return pyjson.load(reader)
    except Exception:
        return []

def write_users_to_hdfs(users):
    with hdfs_client.write(HDFS_USERS_PATH, overwrite=True, encoding='utf-8') as writer:
        pyjson.dump(users, writer)

def get_user_hdfs(username):
    users = read_users_from_hdfs()
    return next((u for u in users if u["username"] == username), None)

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

def authenticate_user_hdfs(username, password):
    user = get_user_hdfs(username)
    if not user or not verify_password(password, user["hashed_password"]):
        return None
    return user

def create_access_token(data: dict, expires_delta=None):
    from datetime import timedelta
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme)):
    print("TOKEN REÇU:", token)
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    user = get_user_hdfs(username)
    if user is None:
        raise credentials_exception
    return user

# ────── UTILS HDFS: PRODUITS ET LOGS ──────
def read_products_from_hdfs():
    try:
        with hdfs_client.read(HDFS_PRODUCTS_PATH) as reader:
            return pyjson.load(reader)
    except Exception:
        return []

def write_products_to_hdfs(products):
    with hdfs_client.write(HDFS_PRODUCTS_PATH, overwrite=True, encoding='utf-8') as writer:
        pyjson.dump(products, writer)

def get_product_hdfs(barcode):
    return next((p for p in read_products_from_hdfs() if p["barcode"] == barcode), None)

def add_product_hdfs(product):
    products = read_products_from_hdfs()
    products.append(product)
    write_products_to_hdfs(products)

def append_log_to_hdfs(log):
    try:
        with hdfs_client.read(HDFS_LOGS_PATH) as reader:
            logs = pyjson.load(reader)
    except Exception:
        logs = []
    logs.append(log)
    with hdfs_client.write(HDFS_LOGS_PATH, overwrite=True, encoding='utf-8') as writer:
        pyjson.dump(logs, writer)

# ────── KAFKA ──────
@app.on_event("startup")
async def on_startup():
    global producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    logger.info(f"Kafka Producer prêt sur {KAFKA_BOOTSTRAP_SERVERS}")

def send_product_to_kafka(data):
    if not producer:
        logger.warning("Kafka producer non initialisé")
        return
    try:
        producer.send(KAFKA_TOPIC, data)
        producer.flush()
        logger.info("Donnée envoyée à Kafka")
    except Exception as e:
        logger.error(f"Erreur Kafka : {e}")

# ────── ROUTES API ──────
@app.post("/signup", status_code=201)
async def signup(username: str, full_name: str, password: str):
    users = read_users_from_hdfs()
    if any(u["username"] == username for u in users):
        raise HTTPException(409, "Nom d'utilisateur déjà pris")
    hashed = get_password_hash(password)
    users.append({"username": username, "full_name": full_name, "hashed_password": hashed})
    write_users_to_hdfs(users)
    return {"username": username, "status": "created"}

@app.post("/login")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user_hdfs(form_data.username, form_data.password)
    if not user:
        raise HTTPException(400, "Nom d'utilisateur ou mot de passe incorrect")
    access_token = create_access_token(data={"sub": user["username"]})
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/event")
async def receive_event(event: dict, current_user=Depends(get_current_user)):
    event["user_id"] = current_user["username"]
    event["timestamp"] = datetime.now(timezone.utc).isoformat()
    send_product_to_kafka(event)
    append_log_to_hdfs(event)
    return {"status": "event_received"}

@app.get("/product/{barcode}")
async def get_product(barcode: str, essential: bool = Query(False), current_user=Depends(get_current_user)):
    prod = get_product_hdfs(barcode)
    if prod:
        data = prod["data"]
        logger.info(f"[HDFS] Produit {barcode} trouvé")
    else:
        url = f"https://world.openfoodfacts.net/api/v2/product/{barcode}.json"
        async with httpx.AsyncClient() as client:
            resp = await client.get(url)
            if resp.status_code == 404:
                raise HTTPException(404, "Produit non trouvé")
            data = resp.json()
        add_product_hdfs({"barcode": barcode, "data": data})
        logger.info(f"[API→HDFS] Produit {barcode} ajouté")

    event = {
        "event": "product_view",
        "user_id": current_user["username"],
        "barcode": barcode,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    send_product_to_kafka(event)
    append_log_to_hdfs(event)

    if essential:
        prod_data = data.get("product", {})
        nutr = prod_data.get("nutriments", {})
        return {
            "product_name": prod_data.get("product_name", "N/A"),
            "image_url": prod_data.get("image_url"),
            "nutriscore": {
                "grade": prod_data.get("nutriscore_grade", "N/A"),
                "score": prod_data.get("nutriscore_score", "N/A"),
            },
            "ecoscore": {
                "grade": prod_data.get("ecoscore_grade", "N/A"),
                "score": prod_data.get("ecoscore_score", "N/A"),
            },
            "nova_group": prod_data.get("nova_group", "N/A"),
            "health_risks": {
                "fat": nutr.get("fat", "N/A"),
                "saturated_fat": nutr.get("saturated-fat", "N/A"),
                "sugars": nutr.get("sugars", "N/A"),
                "salt": nutr.get("salt", "N/A"),
                "palm_oil": "en:palm-oil" in prod_data.get("ingredients_analysis_tags", []),
                "additives": prod_data.get("additives_tags", []),
                "allergens": prod_data.get("allergens_tags", []),
            }
        }

    return data

@app.post("/products/", status_code=201)
async def add_product(barcode: str = Query(...)):
    if get_product_hdfs(barcode):
        raise HTTPException(409, "Produit déjà existant")
    url = f"https://world.openfoodfacts.net/api/v2/product/{barcode}.json"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        if resp.status_code == 404:
            raise HTTPException(404, "Produit non trouvé")
        data = resp.json()
    add_product_hdfs({"barcode": barcode, "data": data})
    logger.info(f"[POST] Produit {barcode} ajouté à HDFS")
    return {"barcode": barcode, "status": "added"}

@app.get("/recommendation/")
async def get_recommendation(product_name: str = Query(...)):
    query_vec = vectorizer.transform([product_name])
    sims = (sparse.cosine_similarity(query_vec, green_vectors).flatten())
    idx = int(sims.argmax())
    item = green_df.iloc[idx].to_dict()
    score = float(sims[idx])
    return {"recommendation": item, "similarity": score}

router = APIRouter()

@router.get("/meals/{username}")
def get_meals(username: str):
    # Adapte le chemin selon ton montage HDFS ou copie locale
    files = glob.glob(f"/path/to/mounted/hdfs/summary/{username}_meals/part-*.json")
    meals = []
    for file in files:
        with open(file) as f:
            for line in f:
                meals.append(json.loads(line))
    return meals

@router.get("/stats/scans_by_user")
def get_scans_by_user():
    # Adapte le chemin selon ton montage HDFS ou copie locale
    files = glob.glob("/path/to/mounted/hdfs/summary/scans_by_user/part-*.json")
    stats = []
    for file in files:
        with open(file) as f:
            for line in f:
                stats.append(json.loads(line))
    return stats
