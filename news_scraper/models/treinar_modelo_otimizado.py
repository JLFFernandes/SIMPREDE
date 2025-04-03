import pandas as pd
import json
import re
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import classification_report, confusion_matrix
from imblearn.over_sampling import SMOTE
import joblib

# Carregar dataset
df = pd.read_csv("publico_arquivo_todos_artigos_classificados.csv")

# Carregar municípios
localidades = json.load(open("municipios.json", encoding="utf-8"))
localidades = [loc.lower().replace(' ', '-') for loc in localidades]

# Função para extrair features avançadas dos URLs
def extrair_features_avancadas(url):
    return {
        "tem_sociedade": int("/sociedade/noticia/" in url),
        "tem_data": int(bool(re.search(r'/\d{4}/\d{2}/\d{2}/', url))),
        "tem_municipio": int(any(mun in url.lower() for mun in localidades)),
        "url_len": len(url),
        "niveis_url": url.count("/"),
        "texto_url": url.lower()
    }

# Aplicar feature extraction ao dataset
df_features = df['Link'].apply(extrair_features_avancadas).apply(pd.Series)

# TF-IDF para texto dos URLs
vectorizer = TfidfVectorizer(max_features=50)
tfidf_features = vectorizer.fit_transform(df_features['texto_url']).toarray()
tfidf_df = pd.DataFrame(tfidf_features, columns=[f'tfidf_{i}' for i in range(tfidf_features.shape[1])])

# Combinar TF-IDF com outras features
X = pd.concat([df_features.drop(columns=['texto_url']).reset_index(drop=True), tfidf_df], axis=1)
y = df['relevante']

# Dividir dataset
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, random_state=42
)

# Balanceamento com SMOTE
smote = SMOTE(random_state=42)
X_train_smote, y_train_smote = smote.fit_resample(X_train, y_train)

# Grid Search para otimizar hiperparâmetros
param_grid = {
    'n_estimators': [100, 200],
    'max_depth': [None, 10, 20],
    'min_samples_split': [2, 5]
}

grid = GridSearchCV(RandomForestClassifier(random_state=42), param_grid, cv=5, scoring='f1')
grid.fit(X_train_smote, y_train_smote)

# Modelo otimizado
modelo_otimizado = grid.best_estimator_

# Avaliar modelo
y_pred = modelo_otimizado.predict(X_test)
print(classification_report(y_test, y_pred))
print(confusion_matrix(y_test, y_pred))

# Salvar o modelo otimizado
joblib.dump(modelo_otimizado, "modelo_classificacao_urls_otimizado.pkl")
print("✅ Modelo salvo como modelo_classificacao_urls_otimizado.pkl")
