import spacy
from spacy.training.example import Example

# Helper to check offsets (run this before training to debug)
def check_offsets(nlp, train_data):
    for text, ann in train_data:
        ents = ann["entities"]
        for start, end, label in ents:
            span = text[start:end]
            print(f"Text: '{text}' | Entity: '{span}' ({label}) | Offset: ({start}, {end})")
            # print(spacy.training.offsets_to_biluo_tags(nlp.make_doc(text), ents))
            # print(spacy.training.offsets_to_biluo_tags(nlp.make_doc(text), ents))

# Corrigir offsets e adicionar exemplos variados
TRAIN_DATA = [
    # FATALITIES
    ("Duas pessoas morreram na enchente.", {"entities": [(0, 22, "FATALITIES")]}),  # 'Duas pessoas morreram'
    ("Três pessoas morreram.", {"entities": [(0, 21, "FATALITIES")]}),
    ("Uma vítima mortal foi registada.", {"entities": [(0, 19, "FATALITIES")]}),
    ("Morreram cinco pessoas.", {"entities": [(0, 23, "FATALITIES")]}),
    ("O desastre causou quatro mortes.", {"entities": [(20, 34, "FATALITIES")]}),
    ("Foram confirmadas dez mortes.", {"entities": [(18, 30, "FATALITIES")]}),
    ("Houve uma vítima mortal.", {"entities": [(6, 25, "FATALITIES")]}),
    ("Sete pessoas perderam a vida.", {"entities": [(0, 28, "FATALITIES")]}),
    ("O acidente resultou em duas mortes.", {"entities": [(24, 37, "FATALITIES")]}),
    # INJURED
    ("Cinco ficaram feridas após o desabamento.", {"entities": [(0, 22, "INJURED")]}),
    ("Sete pessoas ficaram feridas.", {"entities": [(0, 28, "INJURED")]}),
    ("Houve três feridos.", {"entities": [(6, 21, "INJURED")]}),
    ("O acidente deixou dez feridos.", {"entities": [(20, 32, "INJURED")]}),
    ("Quatro pessoas ficaram feridas.", {"entities": [(0, 31, "INJURED")]}),
    ("Foram registados dois feridos.", {"entities": [(18, 32, "INJURED")]}),
    ("Vinte pessoas sofreram ferimentos.", {"entities": [(0, 34, "INJURED")]}),
    ("O deslizamento provocou oito feridos.", {"entities": [(25, 38, "INJURED")]}),
    # EVACUATED
    ("Vinte pessoas foram evacuadas devido à inundação.", {"entities": [(0, 31, "EVACUATED")]}),
    ("Foram evacuadas dez famílias.", {"entities": [(6, 28, "EVACUATED")]}),
    ("Cerca de 100 pessoas evacuadas.", {"entities": [(10, 34, "EVACUATED")]}),
    ("Mais de cinquenta pessoas evacuadas.", {"entities": [(8, 39, "EVACUATED")]}),
    ("Evacuaram-se trinta moradores.", {"entities": [(0, 30, "EVACUATED")]}),
    ("Dez famílias tiveram de ser evacuadas.", {"entities": [(0, 37, "EVACUATED")]}),
    ("Sessenta pessoas foram retiradas das casas.", {"entities": [(0, 38, "EVACUATED")]}),
    # DISPLACED
    ("Dez moradores ficaram desalojados.", {"entities": [(0, 34, "DISPLACED")]}),
    ("Cerca de 30 pessoas ficaram sem casa.", {"entities": [(9, 41, "DISPLACED")]}),
    ("O temporal deixou vinte desalojados.", {"entities": [(20, 39, "DISPLACED")]}),
    ("Trinta famílias ficaram deslocadas.", {"entities": [(0, 35, "DISPLACED")]}),
    ("Oito pessoas perderam as casas.", {"entities": [(0, 31, "DISPLACED")]}),
    ("Mais de cem pessoas ficaram desalojadas.", {"entities": [(8, 43, "DISPLACED")]}),
    # MISSING
    ("Uma pessoa está desaparecida.", {"entities": [(0, 30, "MISSING")]}),
    ("Duas pessoas continuam desaparecidas.", {"entities": [(0, 37, "MISSING")]}),
    ("Três pessoas estão desaparecidas.", {"entities": [(0, 35, "MISSING")]}),
    ("Há relatos de cinco desaparecidos.", {"entities": [(16, 36, "MISSING")]}),
    ("Ainda falta localizar uma pessoa.", {"entities": [(24, 39, "MISSING")]}),
    ("Quatro moradores estão em paradeiro desconhecido.", {"entities": [(0, 47, "MISSING")]}),
    # Exemplos negativos (sem entidades)
    ("A chuva causou danos materiais.", {"entities": []}),
    ("O rio transbordou mas não houve vítimas.", {"entities": []}),
    ("A população foi alertada para o risco de inundações.", {"entities": []}),
    ("O evento decorreu sem incidentes.", {"entities": []}),
    ("As autoridades monitorizam a situação.", {"entities": []}),
]

def train_ner(output_dir: str = "../google_news_scrapper/models/victims_nlp", n_iter: int = 30):
    nlp = spacy.blank("pt")  # modelo em português
    if "ner" not in nlp.pipe_names:
        ner = nlp.add_pipe("ner")
    else:
        ner = nlp.get_pipe("ner")
    ner.add_label("FATALITIES")
    ner.add_label("INJURED")
    ner.add_label("EVACUATED")
    ner.add_label("DISPLACED")
    ner.add_label("MISSING")

    # Debug: check offsets before training
    # check_offsets(nlp, TRAIN_DATA)

    other_pipes = [pipe for pipe in nlp.pipe_names if pipe != "ner"]
    with nlp.disable_pipes(*other_pipes):
        optimizer = nlp.begin_training()
        for itn in range(n_iter):
            losses = {}
            for text, annotations in TRAIN_DATA:
                example = Example.from_dict(nlp.make_doc(text), annotations)
                nlp.update([example], drop=0.5, losses=losses)
            print(f"Iteration {itn+1}, Losses: {losses}")

    nlp.to_disk(output_dir)
    print(f"Model saved to {output_dir}")

if __name__ == "__main__":
    train_ner()
