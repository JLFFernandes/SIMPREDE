import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.pipeline import Pipeline
import joblib
import os
from nltk.corpus import stopwords
import nltk

# ========= CONFIG ========= #
DATA_PATH = "data/intermediate_google_news_labeled.csv"
MODEL_DIR = "models"
MODEL_FILE = os.path.join(MODEL_DIR, "modelo_classificacao.pkl")
VECTORIZER_FILE = os.path.join(MODEL_DIR, "tfidf_vectorizer.pkl")
TEXT_COLUMN = "title"
LABEL_COLUMN = "label"

# ========= LOAD DATA ========= #
df = pd.read_csv(DATA_PATH)
df.dropna(subset=[TEXT_COLUMN, LABEL_COLUMN], inplace=True)

# ========= TRAIN/TEST SPLIT ========= #
X_train, X_test, y_train, y_test = train_test_split(
    df[TEXT_COLUMN], df[LABEL_COLUMN], test_size=0.2, random_state=42
)

# ========= VECTORIZAÇÃO + MODELO ========= #
# Load Portuguese stop words
try:
    stop_words = stopwords.words('portuguese')
except LookupError:
    nltk.download('stopwords')
    stop_words = stopwords.words('portuguese')

vectorizer = TfidfVectorizer(
    max_features=1000,
    stop_words=stop_words,  # Use custom stop words
    lowercase=True
)

model = LogisticRegression(max_iter=500)

X_train_tfidf = vectorizer.fit_transform(X_train)
X_test_tfidf = vectorizer.transform(X_test)

model.fit(X_train_tfidf, y_train)

# ========= AVALIAÇÃO ========= #
y_pred = model.predict(X_test_tfidf)
print("=== CLASSIFICATION REPORT ===")
print(classification_report(y_test, y_pred))
print("=== CONFUSION MATRIX ===")
print(confusion_matrix(y_test, y_pred))

# ========= SAVE MODELO E VECTORIZADOR ========= #
os.makedirs(MODEL_DIR, exist_ok=True)
joblib.dump(model, MODEL_FILE)
joblib.dump(vectorizer, VECTORIZER_FILE)
print(f"✅ Modelo salvo em {MODEL_FILE}")
print(f"✅ Vetorizador salvo em {VECTORIZER_FILE}")
