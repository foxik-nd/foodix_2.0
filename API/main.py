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

async def get_user_db(session, username: str):
    user = await session.get(UserDB, username)
    return user

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

async def authenticate_user_db(session, username: str, password: str):
    user = await get_user_db(session, username)
    if not user or not verify_password(password, user.hashed_password):
        return None
    return user

def create_access_token(data: dict, expires_delta=None):
    from datetime import datetime, timedelta
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme), session: AsyncSession = Depends(get_session)):
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
    user = await get_user_db(session, username)
    if user is None:
        raise credentials_exception
    return user

@app.on_event("startup")
async def on_startup():
    global producer
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Tables PostgreSQL prêtes")
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info(f"Producer Kafka initialisé sur {kafka_bootstrap_servers}")

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
    current_user: UserDB = Depends(get_current_user),
    session: AsyncSession = Depends(get_session)
):
    persisted = await session.get(Product, barcode)
    if persisted:
        data = persisted.data
        logger.info(f"[DB] produit {barcode} trouvé en base")
    else:
        url = f"https://world.openfoodfacts.net/api/v2/product/{barcode}.json"
        async with httpx.AsyncClient() as client:
            resp = await client.get(url)
            if resp.status_code == 404:
                raise HTTPException(404, "Produit non trouvé")
            resp.raise_for_status()
            data = resp.json()
        prod = Product(barcode=barcode, data=data)
        session.add(prod)
        await session.commit()
        logger.info(f"[API→DB] produit {barcode} récupéré et stocké")
    user_event = {
        "event": "product_view",
        "user_id": current_user.username,
        "barcode": barcode,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    send_product_to_kafka(user_event)
    send_product_to_kafka(data)

    if essential:
        prod = data.get("product", {})
        nutr = prod.get("nutriments", {})
        return {
            "product_name": prod.get("product_name", "N/A"),
            "image_url": prod.get("image_url"),
            "nutriscore": {
                "grade": prod.get("nutriscore_grade", "N/A"),
                "score": prod.get("nutriscore_score", "N/A"),
            },
            "ecoscore": {
                "grade": prod.get("ecoscore_grade", "N/A"),
                "score": prod.get("ecoscore_score", "N/A"),
            },
            "nova_group": prod.get("nova_group", "N/A"),
            "health_risks": {
                "fat": nutr.get("fat", "N/A"),
                "saturated_fat": nutr.get("saturated-fat", "N/A"),
                "sugars": nutr.get("sugars", "N/A"),
                "salt": nutr.get("salt", "N/A"),
                "palm_oil": "en:palm-oil" in prod.get("ingredients_analysis_tags", []),
                "additives": prod.get("additives_tags", []),
                "allergens": prod.get("allergens_tags", []),
            }
        }

    return data

@app.post("/products/", status_code=201)
async def add_product(
    barcode: str = Query(..., description="Code-barres du produit à ajouter"),
    session: AsyncSession = Depends(get_session)
):
    if await session.get(Product, barcode):
        raise HTTPException(409, "Produit déjà existant")
    url = f"https://world.openfoodfacts.net/api/v2/product/{barcode}.json"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        if resp.status_code == 404:
            raise HTTPException(404, "Produit non trouvé")
        resp.raise_for_status()
        data = resp.json()
    prod = Product(barcode=barcode, data=data)
    session.add(prod)
    await session.commit()
    logger.info(f"[POST] produit {barcode} ajouté en base")
    send_product_to_kafka(data)
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
    password: str = Query(...),
    session: AsyncSession = Depends(get_session)
):
    hashed_password = get_password_hash(password)
    user = UserDB(username=username, full_name=full_name, hashed_password=hashed_password)
    session.add(user)
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        raise HTTPException(409, "Nom d'utilisateur déjà pris")
    return {"username": username, "status": "created"}

@app.post("/login")
async def login(form_data: OAuth2PasswordRequestForm = Depends(), session: AsyncSession = Depends(get_session)):
    user = await authenticate_user_db(session, form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    access_token = create_access_token(data={"sub": user.username})
    return {"access_token": access_token, "token_type": "bearer"}