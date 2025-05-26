import spacy
from spacy.training.example import Example
from spacy.pipeline import EntityRuler
import os

# Training data with very specific patterns and strict boundaries
TRAIN_DATA = [
    # FATALITIES - death-specific patterns
    ("Duas pessoas morreram.", {"entities": [(0, 21, "FATALITIES")]}),
    ("Três mortes confirmadas.", {"entities": [(0, 21, "FATALITIES")]}),
    ("Uma vítima mortal.", {"entities": [(0, 16, "FATALITIES")]}),
    ("Cinco óbitos registrados.", {"entities": [(0, 22, "FATALITIES")]}),
    ("Mortes foram confirmadas.", {"entities": [(0, 21, "FATALITIES")]}),
    ("Número de mortos: dois.", {"entities": [(10, 16, "FATALITIES")]}),
    ("Três vítimas fatais identificadas.", {"entities": [(0, 18, "FATALITIES")]}),
    ("Confirmaram quatro mortes.", {"entities": [(12, 25, "FATALITIES")]}),
    ("As vítimas não sobreviveram.", {"entities": [(3, 10, "FATALITIES")]}),
    ("Perderam a vida duas pessoas.", {"entities": [(0, 27, "FATALITIES")]})
]

# Pattern rules for the entity ruler
FATALITIES_PATTERNS = [
    {"label": "FATALITIES", "pattern": [{"LOWER": {"IN": ["morreu", "morreram", "mortos", "óbitos", "vítimas"]}}, {"POS": "NUM"}]},
    {"label": "FATALITIES", "pattern": [{"POS": "NUM"}, {"LOWER": {"IN": ["mortes", "óbitos", "vítimas", "mortos", "falecidos"]}}]}
]

INJURED_PATTERNS = [
    {"label": "INJURED", "pattern": [{"LOWER": "feridos"}]},
    {"label": "INJURED", "pattern": [{"LOWER": {"IN": ["ferido", "ferida", "feridos", "feridas"]}}, {"POS": "ADJ"}]},
    {"label": "INJURED", "pattern": [{"POS": "NUM"}, {"LOWER": {"IN": ["ferido", "ferida", "feridos", "feridas"]}}]}
]

EVACUATED_PATTERNS = [
    {"label": "EVACUATED", "pattern": [{"LOWER": "evacuados"}]},
    {"label": "EVACUATED", "pattern": [{"LOWER": {"IN": ["evacuada", "evacuadas", "evacuados"]}}, {"POS": "ADJ"}]},
    {"label": "EVACUATED", "pattern": [{"LOWER": "retirados"}]}
]

DISPLACED_PATTERNS = [
    {"label": "DISPLACED", "pattern": [{"LOWER": "desalojados"}]},
    {"label": "DISPLACED", "pattern": [{"LOWER": {"IN": ["desalojado", "desalojada", "desalojados", "desalojadas"]}}]},
    {"label": "DISPLACED", "pattern": [{"LOWER": {"IN": ["desabrigado", "desabrigada", "desabrigados", "desabrigadas"]}}]}
]

MISSING_PATTERNS = [
    {"label": "MISSING", "pattern": [{"LOWER": "desaparecidos"}]},
    {"label": "MISSING", "pattern": [{"LOWER": {"IN": ["desaparecido", "desaparecida", "desaparecidos", "desaparecidas"]}}]},
    {"label": "MISSING", "pattern": [{"POS": "NUM"}, {"LOWER": {"IN": ["desaparecido", "desaparecida", "desaparecidos", "desaparecidas"]}}]}
]

def train_ner(output_dir: str = "../google_news_scrapper/models/victims_nlp", n_iter: int = 150):
    """Train NER model with pattern matching and statistical training."""
    os.makedirs(output_dir, exist_ok=True)
    
    nlp = spacy.blank("pt")
    
    # Add required components in order
    nlp.add_pipe("tagger")
    nlp.add_pipe("attribute_ruler")
    
    # Add NER component
    if "ner" not in nlp.pipe_names:
        ner = nlp.add_pipe("ner")
    else:
        ner = nlp.get_pipe("ner")
        
    # Add entity ruler with patterns
    ruler = nlp.add_pipe("entity_ruler", before="ner")
    patterns = (FATALITIES_PATTERNS + INJURED_PATTERNS + EVACUATED_PATTERNS + 
               DISPLACED_PATTERNS + MISSING_PATTERNS)
    ruler.add_patterns(patterns)
    
    # Add labels
    for label in ["FATALITIES", "INJURED", "EVACUATED", "DISPLACED", "MISSING"]:
        ner.add_label(label)

    # Training
    other_pipes = [pipe for pipe in nlp.pipe_names if pipe != "ner"]
    with nlp.disable_pipes(*other_pipes):
        nlp.begin_training()
        print("Starting training...")
        for itn in range(n_iter):
            losses = {}
            examples = []
            for text, annotations in TRAIN_DATA:
                doc = nlp.make_doc(text)
                example = Example.from_dict(doc, annotations)
                examples.append(example)
            # Lower dropout and more iterations for stable training
            nlp.update(examples, drop=0.1, losses=losses)
            if (itn + 1) % 10 == 0:
                print(f"Iteration {itn+1}/{n_iter}, Losses: {losses}")

    nlp.to_disk(output_dir)
    print(f"\nTraining completed. Model saved to {output_dir}")

if __name__ == "__main__":
    print("Training NER model with pattern matching and statistical learning...")
    train_ner()
