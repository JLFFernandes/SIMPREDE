import pandas as pd
import spacy
import nltk
import re
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, roc_curve
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from imblearn.over_sampling import SMOTE
import matplotlib.pyplot as plt
import joblib

# Downloads necessários
nltk.download('stopwords')
from nltk.corpus import stopwords
stop_words = set(stopwords.words('portuguese'))

# Carregar spaCy em português
nlp = spacy.load("pt_core_news_sm")

# Carregar dados
df = pd.read_csv("publico_arquivo_todos_artigos_classificados.csv")

# Pré-processamento avançado com Lemmatização
def preprocess_text(text):
    doc = nlp(text.lower())
    tokens = [token.lemma_ for token in doc if token.is_alpha and token.text not in stop_words]
    return " ".join(tokens)

df['clean_text'] = df['Texto do Artigo'].apply(preprocess_text)

# Vetorização TF-IDF
vectorizer = TfidfVectorizer(max_features=5000, ngram_range=(1,2))
X_vec = vectorizer.fit_transform(df['clean_text'])
y = df['relevante']

# Balanceamento com SMOTE
smote = SMOTE(random_state=42)
X_balanced, y_balanced = smote.fit_resample(X_vec, y)

# Divisão treino/teste
X_train, X_test, y_train, y_test = train_test_split(
    X_balanced, y_balanced, test_size=0.25, random_state=42
)

# Grid Search Logistic Regression
param_grid = {'C': [0.1, 1, 10], 'solver': ['liblinear', 'lbfgs'], 'max_iter': [100, 200]}
grid = GridSearchCV(LogisticRegression(), param_grid, cv=5, scoring='f1')
grid.fit(X_train, y_train)

best_model_lr = grid.best_estimator_
y_pred_lr = best_model_lr.predict(X_test)

# Avaliação Logistic Regression
print("Logistic Regression Classification Report:")
print(classification_report(y_test, y_pred_lr))

# ROC Curve
probs_lr = best_model_lr.predict_proba(X_test)[:, 1]
auc_lr = roc_auc_score(y_test, probs_lr)
fpr_lr, tpr_lr, _ = roc_curve(y_test, probs_lr)

plt.figure(figsize=(8,6))
plt.plot(fpr_lr, tpr_lr, label=f"Logistic Regression (AUC = {auc_lr:.2f})")
plt.plot([0, 1], [0, 1], linestyle="--", label="Baseline")
plt.xlabel("False Positive Rate")
plt.ylabel("True Positive Rate")
plt.title("ROC Curve - Logistic Regression")
plt.legend()
plt.show()

# Random Forest Classifier
model_rf = RandomForestClassifier(n_estimators=100, random_state=42)
model_rf.fit(X_train, y_train)
y_pred_rf = model_rf.predict(X_test)

print("\nRandom Forest Classification Report:")
print(classification_report(y_test, y_pred_rf))

# XGBoost Classifier
model_xgb = XGBClassifier(use_label_encoder=False, eval_metric='logloss')
model_xgb.fit(X_train, y_train)
y_pred_xgb = model_xgb.predict(X_test)

print("\nXGBoost Classification Report:")
print(classification_report(y_test, y_pred_xgb))

# Guardar o melhor modelo e vetor
joblib.dump(best_model_lr, "modelo_classificacao.pkl")
joblib.dump(vectorizer, "tfidf_vectorizer.pkl")

print("\n✅ Modelos treinados e guardados com sucesso!")
