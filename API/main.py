import os
import logging

from fastapi import FastAPI, HTTPException, Query, Depends
import httpx
import pandas as pd
import pickle
from scipy import sparse

from sqlalchemy import Column, String, JSON, DateTime, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

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

@app.on_event("startup")
async def on_startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Tables PostgreSQL prêtes")

@app.get("/product/{barcode}")
async def get_product(
    barcode: str,
    essential: bool = Query(False),
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
    return {"barcode": barcode, "status": "added"}

@app.get("/recommendation/")
async def get_recommendation(product_name: str = Query(...)):
    query_vec = vectorizer.transform([product_name])
    sims = (sparse.cosine_similarity(query_vec, green_vectors).flatten())
    idx  = int(sims.argmax())
    item = green_df.iloc[idx].to_dict()
    score = float(sims[idx])
    return {"recommendation": item, "similarity": score}
