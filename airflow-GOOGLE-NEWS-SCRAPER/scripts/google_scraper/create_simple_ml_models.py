import pickle
import os
from pathlib import Path
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.multiclass import OneVsRestClassifier

def create_hydromorphological_models():
    """Create ML models specifically for hydromorphological event classification"""
    
    # Create models directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    models_dir = Path(script_dir) / "models"
    models_dir.mkdir(exist_ok=True)
    
    print(f"Creating hydromorphological event models in: {models_dir}")
    
    # Comprehensive hydromorphological event samples in Portuguese
    hydro_samples = {
        "inundacao_fluvial": [
            "rio transborda inunda casas evacuação urgente",
            "cheia do tejo deixa desalojados centro cidade",
            "ribeira sai do leito alaga campos agricultores",
            "curso de água galga margens corta estradas",
            "rio mondego provoca inundações coimbra evacuados",
            "cheia súbita arasta carros mata pessoas",
            "caudal excepcional ribeiro inunda vale",
            "rio douro atinge cota alerta população retirada"
        ],
        "inundacao_pluvial": [
            "chuva torrencial alaga ruas centro cidade",
            "aguaceiro intenso provoca inundações urbanas",
            "precipitação extrema satura sistema drenagem",
            "chuva forte causa alagamentos caves garagens",
            "temporal pluvioso inunda túneis metro cortado",
            "precipitação abundante provoca cheias urbanas",
            "chuva persistente alaga estações metro",
            "aguaceiro causa transtornos trânsito lisboa"
        ],
        "inundacao_costeira": [
            "maré viva inunda marginal cascais",
            "ressaca do mar invade casas primeira linha",
            "ondulação forte galgou paredão maritimo",
            "tsunami provocou evacuação zona costeira",
            "maremoto causa ondas gigantes destroi cais",
            "tempestade marítima inunda baixa pombalina",
            "agitação marítima forte galga molhes",
            "mar invade estradas costeiras fecha acesso"
        ],
        "deslizamento": [
            "deslizamento terras mata família casa soterrada",
            "derrocada encosta corta estrada nacional",
            "escorregamento vertente destrói habitações",
            "movimento massa rochosa provoca vítimas",
            "deslize terras provocado chuvas intensas",
            "desprendimento rochas fecha túnel feridos",
            "solo instável provoca derrocada mortal",
            "encosta desliza sobre aldeia evacuação total"
        ],
        "erosao": [
            "erosão costeira destrói casas beira mar",
            "galgamento ondas erode falésia habitações",
            "mar avança terra dentro come praia",
            "erosão fluvial provoca derrocada ponte",
            "margem rio cede casa cai água",
            "fenómenos erosivos ameaçam povoação costeira",
            "recuo costa ameaça infraestruturas balneares",
            "processo erosivo acelera destruição marginal"
        ],
        "subsidencia": [
            "subsidência terreno engole casa família",
            "abatimento solo provoca colapso edifício",
            "cavidade subterrânea cede estrada afunda",
            "terreno cede súbita abertura cratera",
            "solo abate casa desaba interior terra",
            "fenómeno subsidência afeta zona urbana",
            "abatimento súbito terreno isola casas",
            "cavidade natural provoca afundamento via"
        ]
    }
    
    # Non-hydromorphological events for contrast
    non_hydro_samples = [
        "incêndio florestal combatido bombeiros meios aéreos",
        "vento forte derruba árvores corta electricidade",
        "granizo destrói culturas vinhas olivais",
        "seca prolongada afeta agricultura reservatórios",
        "neve abundante bloqueia estradas montanha",
        "tempestade seca raios provocam incêndios",
        "notícia desporto futebol jogo resultado vitória",
        "política economia governo ministro decisão",
        "cultura arte exposição museu evento festival",
        "tecnologia internet aplicação novo sistema",
        "saúde médico hospital tratamento paciente",
        "educação escola universidade estudantes",
        "turismo férias viagem destino praia hotel",
        "música festival concerto artista espetáculo"
    ]
    
    # Prepare training data
    texts = []
    labels = []
    
    # Add hydromorphological samples
    for event_type, samples in hydro_samples.items():
        texts.extend(samples)
        labels.extend([event_type] * len(samples))
    
    # Add non-hydromorphological samples
    texts.extend(non_hydro_samples)
    labels.extend(['non_hydro'] * len(non_hydro_samples))
    
    print(f"Training data: {len(texts)} samples")
    print(f"Hydromorphological classes: {list(hydro_samples.keys())}")
    print(f"Class distribution:")
    for label in set(labels):
        count = labels.count(label)
        print(f"  {label}: {count} samples")
    
    # Create enhanced vectorizer for hydromorphological terms
    vectorizer = TfidfVectorizer(
        max_features=2000,
        stop_words=None,
        ngram_range=(1, 3),  # Include trigrams for better context
        min_df=1,
        max_df=0.95,
        analyzer='word',
        lowercase=True,
        # Custom token pattern to capture water-related terms
        token_pattern=r'\b[a-záàâãéêíóôõúçñ]+\b'
    )
    
    X = vectorizer.fit_transform(texts)
    print(f"Feature matrix shape: {X.shape}")
    
    # Create sophisticated classifier for hydromorphological events
    classifier = LogisticRegression(
        random_state=42,
        max_iter=2000,
        multi_class='ovr',
        class_weight='balanced',  # Handle any class imbalance
        solver='liblinear'  # Good for small datasets
    )
    classifier.fit(X, labels)
    
    # Save vectorizer
    vectorizer_path = models_dir / "tfidf_vectorizer.pkl"
    with open(vectorizer_path, 'wb') as f:
        pickle.dump(vectorizer, f)
    print(f"✅ Created hydromorphological TF-IDF vectorizer: {vectorizer_path}")
    
    # Save classifier
    model_path = models_dir / "modelo_classificacao.pkl"
    with open(model_path, 'wb') as f:
        pickle.dump(classifier, f)
    print(f"✅ Created hydromorphological classification model: {model_path}")
    
    # Test the models with hydromorphological examples
    print("\n🧪 Testing hydromorphological event classification...")
    
    test_texts = [
        "rio sai do leito inunda vila evacuação",
        "chuva intensa causa alagamentos centro cidade",
        "mar galga paredão inunda estrada marginal",
        "deslizamento terras mata pessoas casa soterrada",
        "erosão costeira destrói casas beira mar",
        "terreno abate casa desaba cratera",
        "incêndio florestal bombeiros combatem chamas",
        "jogo de futebol benfica porto resultado"
    ]
    
    test_vectors = vectorizer.transform(test_texts)
    predictions = classifier.predict(test_vectors)
    probabilities = classifier.predict_proba(test_vectors)
    
    print(f"✅ Test predictions shape: {predictions.shape}")
    print(f"✅ Test probabilities shape: {probabilities.shape}")
    
    # Show detailed predictions for hydromorphological events
    class_names = classifier.classes_
    print(f"\nDetailed test results:")
    for i, (text, pred) in enumerate(zip(test_texts, predictions)):
        # Get confidence for predicted class
        pred_idx = list(class_names).index(pred)
        confidence = probabilities[i][pred_idx]
        print(f"  '{text[:50]}...'")
        print(f"    -> {pred} (confidence: {confidence:.3f})")
        
        # Show top 3 predictions if it's a hydromorphological event
        if pred != 'non_hydro':
            top_3_idx = np.argsort(probabilities[i])[-3:][::-1]
            print(f"    Top 3: {[(class_names[idx], probabilities[i][idx]) for idx in top_3_idx]}")
        print()
    
    print("✅ Hydromorphological event classification models created successfully!")
    return True

# Also create a simplified rule-based detector for hydromorphological events
def create_hydro_rule_patterns():
    """Create rule-based patterns specifically for hydromorphological events"""
    
    patterns = {
        "inundacao_fluvial": {
            "primary_keywords": ["rio", "ribeira", "ribeiro", "cheia", "transborda", "galga"],
            "secondary_keywords": ["curso água", "leito", "margens", "caudal", "afluente"],
            "impact_keywords": ["inunda", "alaga", "evacuação", "desalojados", "isolados"]
        },
        "inundacao_pluvial": {
            "primary_keywords": ["chuva", "precipitação", "aguaceiro", "temporal"],
            "secondary_keywords": ["torrencial", "intensa", "abundante", "drenagem"],
            "impact_keywords": ["alaga", "inunda", "caves", "túneis", "sistema drenagem"]
        },
        "inundacao_costeira": {
            "primary_keywords": ["mar", "maré", "ondas", "ressaca", "tsunami"],
            "secondary_keywords": ["costeira", "marginal", "paredão", "molhe", "agitação marítima"],
            "impact_keywords": ["galga", "invade", "inunda", "galgamento", "evacuação"]
        },
        "deslizamento": {
            "primary_keywords": ["deslizamento", "derrocada", "escorregamento", "deslize"],
            "secondary_keywords": ["encosta", "vertente", "talude", "massa rochosa", "terrenos"],
            "impact_keywords": ["mata", "soterra", "corta estrada", "destrói", "vítimas"]
        },
        "erosao": {
            "primary_keywords": ["erosão", "galgamento", "recuo", "desgaste"],
            "secondary_keywords": ["costeira", "falésia", "margem", "praia", "costa"],
            "impact_keywords": ["destrói", "ameaça", "avança", "come", "recua"]
        },
        "subsidencia": {
            "primary_keywords": ["subsidência", "abatimento", "afundamento", "colapso"],
            "secondary_keywords": ["terreno", "solo", "cavidade", "cratera"],
            "impact_keywords": ["engole", "cede", "afunda", "desaba", "provoca"]
        }
    }
    
    # Save patterns for use in processing
    script_dir = os.path.dirname(os.path.abspath(__file__))
    patterns_path = Path(script_dir) / "hydro_patterns.pkl"
    
    with open(patterns_path, 'wb') as f:
        pickle.dump(patterns, f)
    
    print(f"✅ Created hydromorphological rule patterns: {patterns_path}")
    return patterns

if __name__ == "__main__":
    # Create both ML models and rule patterns
    create_hydromorphological_models()
    create_hydro_rule_patterns()
    
    print("\n🎯 Hydromorphological event classification system created!")
    print("📊 Event types covered:")
    print("  - inundacao_fluvial (river flooding)")
    print("  - inundacao_pluvial (pluvial flooding)")  
    print("  - inundacao_costeira (coastal flooding)")
    print("  - deslizamento (landslides)")
    print("  - erosao (erosion)")
    print("  - subsidencia (subsidence)")