
# GOOD FOOD

green_df.csv : dataset produits "verts" ( good products)
green_vectors.npz : matrice vectorielle TF-IDF des noms de produits
vectorizer.pkl: model TfidfVectorizer entraîné sur les noms (product nameh)

# running function
```python
import pandas as pd
from scipy import sparse
import pickle
from sklearn.metrics.pairwise import cosine_similarity

with open('vectorizer.pkl', 'rb') as f:
    vectorizer = pickle.load(f)

# testing a product
def recommend(product_name):
    query_vec = vectorizer.transform([product_name])
    similarities = cosine_similarity(query_vec, green_vectors).flatten()
    best_idx = similarities.argmax()
    return green_df.iloc[best_idx], similarities[best_idx]
```






