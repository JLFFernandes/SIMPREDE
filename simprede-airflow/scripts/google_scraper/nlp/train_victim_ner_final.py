import spacy
from spacy.training.example import Example
import os

# Training data with refined boundaries and more precise pattern matching
TRAIN_DATA = [
    # FATALITIES - focused on death-specific terminology
    ("Duas pessoas morreram na enchente.", {"entities": [(0, 21, "FATALITIES")]}),
    ("O temporal causou três mortes.", {"entities": [(19, 30, "FATALITIES")]}),
    ("Uma vítima mortal foi confirmada.", {"entities": [(0, 16, "FATALITIES")]}),
    ("Número de mortos chegou a cinco.", {"entities": [(10, 16, "FATALITIES")]}),
    ("Houve três óbitos no acidente.", {"entities": [(6, 12, "FATALITIES")]}),
    ("Foram registradas duas mortes.", {"entities": [(17, 23, "FATALITIES")]}),
    ("Duas vítimas fatais no local.", {"entities": [(0, 19, "FATALITIES")]}),
    ("Cinco pessoas faleceram hoje.", {"entities": [(0, 26, "FATALITIES")]}),
    ("O acidente causou mortes.", {"entities": [(19, 25, "FATALITIES")]}),
    ("As vítimas mortais foram identificadas.", {"entities": [(3, 17, "FATALITIES")]})
    ("A tragédia deixou três mortos.", {"entities": [(20, 26, "FATALITIES")]}),
    ("O número de óbitos subiu.", {"entities": [(13, 19, "FATALITIES")]})
    ("Confirmaram as mortes no local.", {"entities": [(14, 20, "FATALITIES")]}),
    
    # INJURED - strict patterns to avoid false positives
    ("Três pessoas ficaram feridas.", {"entities": [(0, 27, "INJURED")]}),
    ("Os feridos foram atendidos.", {"entities": [(3, 10, "INJURED")]}),
    ("Duas pessoas sofreram ferimentos.", {"entities": [(0, 29, "INJURED")]}),
    ("Vários feridos no local.", {"entities": [(0, 14, "INJURED")]}),
    ("Atenderam cinco pessoas feridas.", {"entities": [(10, 27, "INJURED")]}),
    ("O acidente deixou feridos.", {"entities": [(19, 26, "INJURED")]}),
    ("Pessoas com ferimentos leves.", {"entities": [(12, 28, "INJURED")]}),
    ("Os feridos foram socorridos.", {"entities": [(3, 10, "INJURED")]}),
    
    # EVACUATED - clear distinction from DISPLACED
    ("As pessoas foram evacuadas.", {"entities": [(14, 23, "EVACUATED")]}),
    ("Bombeiros evacuaram a área.", {"entities": [(10, 19, "EVACUATED")]}),
    ("Moradores foram retirados do local.", {"entities": [(14, 23, "EVACUATED")]}),
    ("A evacuação foi preventiva.", {"entities": [(2, 11, "EVACUATED")]}),
    ("Todos foram evacuados.", {"entities": [(10, 19, "EVACUATED")]}),
    ("Autoridades retiraram famílias.", {"entities": [(12, 21, "EVACUATED")]}),
    
    # DISPLACED - focusing on longer-term displacement
    ("Famílias ficaram desalojadas.", {"entities": [(9, 27, "DISPLACED")]}),
    ("Pessoas estão sem moradia.", {"entities": [(12, 23, "DISPLACED")]}),
    ("Os desabrigados foram acolhidos.", {"entities": [(3, 14, "DISPLACED")]}),
    ("Muitos perderam suas casas.", {"entities": [(7, 24, "DISPLACED")]}),
    ("Dezenas ficaram desalojados.", {"entities": [(9, 27, "DISPLACED")]}),
    ("Famílias sem residência fixa.", {"entities": [(9, 25, "DISPLACED")]}),
    
    # MISSING - clear patterns for missing persons
    ("Pessoa continua desaparecida.", {"entities": [(0, 24, "MISSING")]}),
    ("Três desaparecidos até agora.", {"entities": [(0, 17, "MISSING")]}),
    ("Procuram por desaparecidos.", {"entities": [(12, 25, "MISSING")]}),
    ("Uma criança está desaparecida.", {"entities": [(0, 27, "MISSING")]}),
    ("Bombeiros buscam desaparecido.", {"entities": [(15, 27, "MISSING")]}),
    ("Ainda procuram os desaparecidos.", {"entities": [(15, 28, "MISSING")]}),
    
    # Negative examples - more focused on similar but non-matching patterns
    ("A chuva causou danos.", {"entities": []}),
    ("O rio transbordou.", {"entities": []}),
    ("A situação é grave.", {"entities": []}),
    ("Bombeiros avaliam danos.", {"entities": []}),
    ("Casas foram inundadas.", {"entities": []}),
    ("O temporal causou estragos.", {"entities": []}),
    ("As estradas bloqueadas.", {"entities": []}),
    ("Equipes no local.", {"entities": []}),
    ("Previsão de mais chuva.", {"entities": []}),
    ("O nível do rio subiu.", {"entities": []}),
    ("Áreas alagadas.", {"entities": []})
]

def train_ner(output_dir: str = "../models/victims_nlp", n_iter: int = 50):
    """Train the NER model with refined dataset using more iterations."""
    os.makedirs(output_dir, exist_ok=True)
    
    nlp = spacy.blank("pt")
    if "ner" not in nlp.pipe_names:
        ner = nlp.add_pipe("ner")
    else:
        ner = nlp.get_pipe("ner")
    
    for label in ["FATALITIES", "INJURED", "EVACUATED", "DISPLACED", "MISSING"]:
        ner.add_label(label)

    # Training with more iterations and adjusted dropout
    other_pipes = [pipe for pipe in nlp.pipe_names if pipe != "ner"]
    with nlp.disable_pipes(*other_pipes):
        nlp.begin_training()
        for itn in range(n_iter):
            losses = {}
            examples = []
            for text, annotations in TRAIN_DATA:
                doc = nlp.make_doc(text)
                example = Example.from_dict(doc, annotations)
                examples.append(example)
            # Lower dropout rate for more stable training
            nlp.update(examples, drop=0.2, losses=losses)
            print(f"Iteration {itn+1}, Losses: {losses}")

    nlp.to_disk(output_dir)
    print(f"Model saved to {output_dir}")

if __name__ == "__main__":
    print("Starting training with refined dataset...")
    train_ner()
