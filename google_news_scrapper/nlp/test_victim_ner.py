import spacy

def test_ner(model_path="/Users/ruicarvalho/Library/CloudStorage/OneDrive-UniversidadeAberta/Uab/Eng_Informática/Ano_3/Semestre_2/21184-Project_LEI/simprede/SIMPREDE/google_news_scrapper/models/victims_nlp"):
    # Load the trained model
    print(f"Loading model from {model_path}")
    try:
        nlp = spacy.load(model_path)
        print("Model loaded successfully!")
    except Exception as e:
        print(f"Error loading model: {e}")
        return

    # Test cases
    test_cases = [
        # FATALITIES
        "A enchente causou cinco mortes na região.",
        "Duas pessoas morreram durante o temporal.",
        "O acidente causou uma vítima mortal.",
        
        # INJURED
        "Quinze pessoas ficaram feridas no deslizamento.",
        "O desabamento deixou três feridos graves.",
        "Vários moradores sofreram ferimentos leves.",
        
        # EVACUATED
        "Cerca de 200 pessoas foram evacuadas da área.",
        "As autoridades evacuaram dez famílias.",
        "Trinta moradores foram retirados das suas casas.",
        
        # DISPLACED
        "Cinquenta famílias ficaram desalojadas.",
        "O temporal deixou 20 pessoas sem casa.",
        "Dezenas de moradores estão desabrigados.",
        
        # MISSING
        "Duas pessoas continuam desaparecidas.",
        "Ainda há três desaparecidos após a enchente.",
        "Uma criança está em paradeiro desconhecido.",
        
        # Mixed and complex cases
        "A tempestade causou duas mortes, deixou 15 feridos e 30 famílias desalojadas.",
        "Três pessoas morreram e dez continuam desaparecidas após o deslizamento.",
        "As autoridades evacuaram 50 pessoas, mas uma continua desaparecida.",
        
        # Negative cases
        "A chuva causou apenas danos materiais.",
        "O rio transbordou mas não houve vítimas.",
        "A situação está sob controle das autoridades."
    ]

    print("\nTesting NER model on sample texts:")
    print("=" * 60)

    for text in test_cases:
        doc = nlp(text)
        print(f"\nText: {text}")
        if doc.ents:
            for ent in doc.ents:
                print(f"Found: {ent.text} ({ent.label_})")
        else:
            print("No entities found.")
        print("-" * 60)

if __name__ == "__main__":
    test_ner()
