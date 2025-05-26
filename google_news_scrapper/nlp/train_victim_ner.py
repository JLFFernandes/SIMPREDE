import spacy
from spacy.training.example import Example

# Helper to check offsets (run this before training to debug)
def check_offsets(nlp, train_data):
    for text, ann in train_data:
        doc = nlp.make_doc(text)
        ents = ann["entities"]
        for start, end, label in ents:
            span = text[start:end]
            print(f"\nText: '{text}'")
            print(f"Entity: '{span}' ({label})")
            print(f"Offset: ({start}, {end})")
            print("Tokens:", [token.text for token in doc])
            print("BILUO tags:", spacy.training.offsets_to_biluo_tags(doc, ents))

# Training data with improved entity boundaries and more examples
TRAIN_DATA = [
    # FATALITIES
    ("Duas pessoas morreram na enchente.", {"entities": [(0, 21, "FATALITIES")]}),
    ("Três pessoas morreram.", {"entities": [(0, 19, "FATALITIES")]}),
    ("Uma vítima mortal foi registada.", {"entities": [(0, 17, "FATALITIES")]}),
    ("Morreram cinco pessoas.", {"entities": [(0, 21, "FATALITIES")]}),
    ("O desastre causou quatro mortes.", {"entities": [(19, 31, "FATALITIES")]}),
    ("Foram confirmadas dez mortes.", {"entities": [(17, 27, "FATALITIES")]}),
    ("Houve uma vítima mortal.", {"entities": [(6, 23, "FATALITIES")]}),
    ("Sete pessoas perderam a vida.", {"entities": [(0, 27, "FATALITIES")]}),
    ("O acidente resultou em duas mortes.", {"entities": [(23, 34, "FATALITIES")]}),
    ("A enchente causou cinco mortes na região.", {"entities": [(13, 25, "FATALITIES")]}),
    ("Uma pessoa morreu durante o temporal.", {"entities": [(0, 11, "FATALITIES")]}),
    ("O número de mortos subiu para três.", {"entities": [(23, 27, "FATALITIES")]}),
    ("Bombeiros confirmam dois óbitos.", {"entities": [(20, 31, "FATALITIES")]}),
    # INJURED
    ("Cinco ficaram feridas após o desabamento.", {"entities": [(0, 20, "INJURED")]}),
    ("Sete pessoas ficaram feridas.", {"entities": [(0, 25, "INJURED")]}),
    ("Houve três feridos.", {"entities": [(6, 17, "INJURED")]}),
    ("O acidente deixou dez feridos.", {"entities": [(19, 29, "INJURED")]}),
    ("Quatro pessoas ficaram feridas.", {"entities": [(0, 27, "INJURED")]}),
    ("Foram registados dois feridos.", {"entities": [(17, 28, "INJURED")]}),
    ("Vinte pessoas sofreram ferimentos.", {"entities": [(0, 32, "INJURED")]}),
    ("O deslizamento provocou oito feridos.", {"entities": [(24, 35, "INJURED")]}),
    ("Quinze pessoas ficaram feridas no deslizamento.", {"entities": [(0, 27, "INJURED")]}),
    ("O desabamento deixou três feridos graves.", {"entities": [(22, 34, "INJURED")]}),
    ("Dois moradores sofreram ferimentos leves.", {"entities": [(0, 34, "INJURED")]}),
    ("O acidente causou dez pessoas feridas.", {"entities": [(15, 31, "INJURED")]}),
    ("Bombeiros socorreram cinco feridos.", {"entities": [(21, 33, "INJURED")]}),
    # EVACUATED
    ("Vinte pessoas foram evacuadas devido à inundação.", {"entities": [(0, 26, "EVACUATED")]}),
    ("Foram evacuadas dez famílias.", {"entities": [(6, 26, "EVACUATED")]}),
    ("Cerca de 100 pessoas evacuadas.", {"entities": [(0, 32, "EVACUATED")]}),
    ("Mais de cinquenta pessoas evacuadas.", {"entities": [(0, 37, "EVACUATED")]}),
    ("Evacuaram-se trinta moradores.", {"entities": [(0, 28, "EVACUATED")]}),
    ("Dez famílias tiveram de ser evacuadas.", {"entities": [(0, 35, "EVACUATED")]}),
    ("Sessenta pessoas foram retiradas das casas.", {"entities": [(0, 32, "EVACUATED")]}),
    ("Autoridades evacuaram duzentos moradores.", {"entities": [(13, 32, "EVACUATED")]}),
    ("Foi necessário evacuar trinta pessoas.", {"entities": [(16, 34, "EVACUATED")]}),
    ("Os bombeiros retiraram quinze famílias.", {"entities": [(14, 34, "EVACUATED")]}),
    ("A área foi evacuada preventivamente.", {"entities": [(0, 24, "EVACUATED")]}),
    ("Moradores foram retirados da zona de risco.", {"entities": [(0, 29, "EVACUATED")]}),
    # DISPLACED
    ("Dez moradores ficaram desalojados.", {"entities": [(0, 32, "DISPLACED")]}),
    ("Cerca de 30 pessoas ficaram sem casa.", {"entities": [(0, 39, "DISPLACED")]}),
    ("O temporal deixou vinte desalojados.", {"entities": [(19, 36, "DISPLACED")]}),
    ("Trinta famílias ficaram deslocadas.", {"entities": [(0, 33, "DISPLACED")]}),
    ("Oito pessoas perderam as casas.", {"entities": [(0, 29, "DISPLACED")]}),
    ("Mais de cem pessoas ficaram desalojadas.", {"entities": [(0, 41, "DISPLACED")]}),
    ("Vinte famílias estão desabrigadas.", {"entities": [(0, 32, "DISPLACED")]}),
    ("A tempestade deixou famílias sem teto.", {"entities": [(14, 34, "DISPLACED")]}),
    ("Dezenas de pessoas perderam suas residências.", {"entities": [(0, 41, "DISPLACED")]}),
    ("Moradores ficaram temporariamente desalojados.", {"entities": [(0, 41, "DISPLACED")]}),
    ("As enchentes deixaram várias famílias sem casa.", {"entities": [(14, 41, "DISPLACED")]}),
    # MISSING
    ("Uma pessoa está desaparecida.", {"entities": [(0, 27, "MISSING")]}),
    ("Duas pessoas continuam desaparecidas.", {"entities": [(0, 34, "MISSING")]}),
    ("Três pessoas estão desaparecidas.", {"entities": [(0, 32, "MISSING")]}),
    ("Há relatos de cinco desaparecidos.", {"entities": [(13, 33, "MISSING")]}),
    ("Ainda falta localizar uma pessoa.", {"entities": [(23, 33, "MISSING")]}),
    ("Quatro moradores estão em paradeiro desconhecido.", {"entities": [(0, 45, "MISSING")]}),
    ("Bombeiros procuram por pessoas desaparecidas.", {"entities": [(16, 37, "MISSING")]}),
    ("Criança continua desaparecida após enchente.", {"entities": [(0, 29, "MISSING")]}),
    ("Duas pessoas não foram localizadas.", {"entities": [(0, 31, "MISSING")]}),
    ("Equipes de resgate buscam desaparecidos.", {"entities": [(23, 36, "MISSING")]}),
    ("Morador permanece em local desconhecido.", {"entities": [(0, 36, "MISSING")]}),
    # Exemplos negativos (sem entidades)
    ("A chuva causou danos materiais.", {"entities": []}),
    ("O rio transbordou mas não houve vítimas.", {"entities": []}),
    ("A população foi alertada para o risco de inundações.", {"entities": []}),
    ("O evento decorreu sem incidentes.", {"entities": []}),
    ("As autoridades monitorizam a situação.", {"entities": []}),
    ("Os danos são apenas materiais.", {"entities": []}),
    ("A situação está sob controle.", {"entities": []}),
    ("Bombeiros avaliam os estragos.", {"entities": []}),
    ("A água invadiu várias casas.", {"entities": []}),
    ("O temporal causou prejuízos.", {"entities": []}),
    ("As estradas foram interditadas.", {"entities": []}),
    ("Equipes trabalham na recuperação.", {"entities": []}),
    ("A previsão é de mais chuva.", {"entities": []}),
    ("O rio está subindo rapidamente.", {"entities": []}),
    ("Várias áreas estão alagadas.", {"entities": []}),
]

def train_ner(output_dir: str = "../google_news_scrapper/models/victims_nlp", n_iter: int = 30):
    # Create output directory if it doesn't exist
    import os
    os.makedirs(output_dir, exist_ok=True)
    
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
        nlp.begin_training()
        for itn in range(n_iter):
            losses = {}
            for text, annotations in TRAIN_DATA:
                example = Example.from_dict(nlp.make_doc(text), annotations)
                nlp.update([example], drop=0.5, losses=losses)
            print(f"Iteration {itn+1}, Losses: {losses}")

    nlp.to_disk(output_dir)
    print(f"Model saved to {output_dir}")

if __name__ == "__main__":
    nlp = spacy.blank("pt")
    print("Checking entity alignments...")
    check_offsets(nlp, TRAIN_DATA)
    print("\nStarting training...")
    train_ner()
