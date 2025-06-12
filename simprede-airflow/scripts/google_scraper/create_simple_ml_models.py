import pickle
import os
from pathlib import Path
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.dummy import DummyClassifier

def create_simple_models():
    """Create simple placeholder ML models for testing"""
    
    # Create models directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    models_dir = Path(script_dir) / "models"
    models_dir.mkdir(exist_ok=True)
    
    print(f"Creating models in: {models_dir}")
    
    # Create a simple TF-IDF vectorizer with Portuguese disaster-related sample texts
    sample_texts = [
        "incêndio florestal mata duas pessoas evacuadas feridos",
        "temporal causa inundações desalojados vitimas",
        "vento forte derruba árvores casas destruídas",
        "chuva intensa evacua população desaparecidos mortos",
        "desastre natural feridos bombeiros socorro",
        "seca prolongada agricultura prejuízos",
        "granizo destrói culturas agrícolas",
        "nevão bloqueia estradas isolados",
        "ciclone aproxima costa evacuação",
        "terramotos tremores população assustada",
        "notícia desporto futebol jogo resultado",
        "política economia governo ministro decisão",
        "cultura arte exposição museu evento",
        "tecnologia internet aplicação novo sistema",
        "saúde médico hospital tratamento"
    ]
    
    # Create and fit vectorizer with Portuguese text
    vectorizer = TfidfVectorizer(
        max_features=1000, 
        stop_words=None,
        ngram_range=(1, 2),  # Include bigrams for better context
        min_df=1,  # Include all terms for small dataset
        max_df=0.95  # Exclude very common terms
    )
    
    # FIT the vectorizer - this is crucial!
    vectorizer.fit(sample_texts)
    
    # Save vectorizer
    vectorizer_path = models_dir / "tfidf_vectorizer.pkl"
    with open(vectorizer_path, 'wb') as f:
        pickle.dump(vectorizer, f)
    print(f"✅ Created TF-IDF vectorizer: {vectorizer_path}")
    
    # Create a simple dummy classifier
    # Transform the sample texts to get feature vectors
    X_sample = vectorizer.transform(sample_texts)
    
    # Labels: 1 = disaster related, 0 = not disaster related
    y_sample = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0]
    
    # Use strategy="stratified" to maintain class distribution
    classifier = DummyClassifier(strategy="stratified", random_state=42)
    classifier.fit(X_sample, y_sample)
    
    # Save classifier
    model_path = models_dir / "modelo_classificacao.pkl"
    with open(model_path, 'wb') as f:
        pickle.dump(classifier, f)
    print(f"✅ Created classification model: {model_path}")
    
    # Test the models to ensure they work
    print("\n🧪 Testing the created models...")
    
    # Test vectorizer
    test_text = ["incêndio mata pessoas", "jogo de futebol"]
    test_vectors = vectorizer.transform(test_text)
    print(f"✅ Vectorizer test: {test_vectors.shape}")
    
    # Test classifier
    predictions = classifier.predict(test_vectors)
    probabilities = classifier.predict_proba(test_vectors)
    print(f"✅ Classifier test - Predictions: {predictions}")
    print(f"✅ Classifier test - Probabilities shape: {probabilities.shape}")
    
    print("✅ Simple ML models created and tested successfully!")
    return True

if __name__ == "__main__":
    create_simple_models()