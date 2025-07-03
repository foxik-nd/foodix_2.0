import os
import logging
import json
from kafka import KafkaProducer
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query, Depends, status
import httpx
import pandas as pd
import pickle
from scipy import sparse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from typing import Optional

from sqlalchemy import Column, String, JSON, DateTime, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import IntegrityError

from hdfs import InsecureClient
import json as pyjson

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@db:5432/products_db"
)

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(
    bind=engine, class_=AsyncSession, expire_on_commit=False
)

Base = declarative_base()

class Product(Base):
    __tablename__ = "products"
    barcode     = Column(String, primary_key=True, index=True)
    data        = Column(JSON)
    created_at  = Column(DateTime(timezone=True), server_default=func.now())
    updated_at  = Column(DateTime(timezone=True),
                          server_default=func.now(),
                          onupdate=func.now())

class UserDB(Base):
    __tablename__ = "users"
    username = Column(String, primary_key=True, index=True)
    full_name = Column(String)
    hashed_password = Column(String)

async def get_session():
    async with AsyncSessionLocal() as session:
        yield session

with open('./model/vectorizer.pkl', 'rb') as f:
    vectorizer = pickle.load(f)
green_df      = pd.read_csv('./model/green_df.csv')
green_vectors = sparse.load_npz('./model/green_vectors.npz')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI()

kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
kafka_topic = os.getenv("KAFKA_TOPIC", "products")
producer = None

SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")

# Config HDFS
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "hdfs")
HDFS_USERS_PATH = "/users/users.json"
HDFS_PRODUCTS_PATH = "/products/products.json"
HDFS_LOGS_PATH = "/logs/user_events.json"
hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)

# --- Gestion utilisateurs sur HDFS ---
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
    for user in users:
        if user['username'] == username:
            return user
    return None

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

def authenticate_user_hdfs(username, password):
    user = get_user_hdfs(username)
    if not user or not verify_password(password, user['hashed_password']):
        return None
    return user

def create_access_token(data: dict, expires_delta=None):
    from datetime import datetime, timedelta
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
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

# --- Gestion produits sur HDFS ---
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
    products = read_products_from_hdfs()
    for prod in products:
        if prod['barcode'] == barcode:
            return prod
    return None

def add_product_hdfs(product):
    products = read_products_from_hdfs()
    products.append(product)
    write_products_to_hdfs(products)

# --- Gestion logs sur HDFS ---
def append_log_to_hdfs(log):
    try:
        logs = []
        with hdfs_client.read(HDFS_LOGS_PATH) as reader:
            logs = pyjson.load(reader)
    except Exception:
        logs = []
    logs.append(log)
    with hdfs_client.write(HDFS_LOGS_PATH, overwrite=True, encoding='utf-8') as writer:
        pyjson.dump(logs, writer)

@app.on_event("startup")
async def on_startup():
    global producer
    # Ajout d'un utilisateur de test si aucun utilisateur n'existe
    users = read_users_from_hdfs()
    if not users:
        users.append({
            "username": "testuser",
            "full_name": "Test User",
            "hashed_password": get_password_hash("testpass")
        })
        write_users_to_hdfs(users)
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info(f"Producer Kafka initialisé sur {kafka_bootstrap_servers}")
    except Exception as e:
        producer = None
        logger.warning(f"Kafka non disponible ({e}), le producer ne sera pas utilisé.")

def send_product_to_kafka(product_data):
    if producer:
        try:
            producer.send(kafka_topic, product_data)
            producer.flush()
            logger.info(f"Produit envoyé sur Kafka topic '{kafka_topic}'")
        except Exception as e:
            logger.error(f"Erreur lors de l'envoi sur Kafka: {e}")
    else:
        logger.warning("Producer Kafka non initialisé")

@app.get("/product/{barcode}")
async def get_product(
    barcode: str,
    essential: bool = Query(False),
    current_user = Depends(get_current_user)
):
    prod = get_product_hdfs(barcode)
    if prod:
        data = prod['data']
        logger.info(f"[HDFS] produit {barcode} trouvé sur HDFS")
    else:
        url = f"https://world.openfoodfacts.net/api/v2/product/{barcode}.json"
        async with httpx.AsyncClient() as client:
            resp = await client.get(url)
            if resp.status_code == 404:
                raise HTTPException(404, "Produit non trouvé")
            resp.raise_for_status()
            data = resp.json()
        add_product_hdfs({"barcode": barcode, "data": data})
        logger.info(f"[API→HDFS] produit {barcode} récupéré et stocké sur HDFS")
    # Log d'événement utilisateur sur HDFS
    user_event = {
        "event": "product_view",
        "user_id": current_user['username'],
        "barcode": barcode,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    append_log_to_hdfs(user_event)
    send_product_to_kafka(user_event)
    send_product_to_kafka(data)
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
async def add_product(
    barcode: str = Query(..., description="Code-barres du produit à ajouter")
):
    if get_product_hdfs(barcode):
        raise HTTPException(409, "Produit déjà existant")
    url = f"https://world.openfoodfacts.net/api/v2/product/{barcode}.json"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        if resp.status_code == 404:
            raise HTTPException(404, "Produit non trouvé")
        resp.raise_for_status()
        data = resp.json()
    add_product_hdfs({"barcode": barcode, "data": data})
    logger.info(f"[POST→HDFS] produit {barcode} ajouté sur HDFS")
    return {"barcode": barcode, "status": "added"}

@app.get("/recommendation/")
async def get_recommendation(product_name: str = Query(...)):
    query_vec = vectorizer.transform([product_name])
    sims = (sparse.cosine_similarity(query_vec, green_vectors).flatten())
    idx  = int(sims.argmax())
    item = green_df.iloc[idx].to_dict()
    score = float(sims[idx])
    return {"recommendation": item, "similarity": score}

@app.post("/signup", status_code=201)
async def signup(
    username: str = Query(...),
    full_name: str = Query(...),
    password: str = Query(...)
):
    users = read_users_from_hdfs()
    if any(u['username'] == username for u in users):
        raise HTTPException(409, "Nom d'utilisateur déjà pris")
    hashed_password = get_password_hash(password)
    users.append({"username": username, "full_name": full_name, "hashed_password": hashed_password})
    write_users_to_hdfs(users)
    return {"username": username, "status": "created"}

@app.post("/login")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user_hdfs(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    access_token = create_access_token(data={"sub": user['username']})
    return {"access_token": access_token, "token_type": "bearer"}