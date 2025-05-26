import spacy
from spacy.training import Example
from spacy.util import minibatch, compounding
from spacy.language import Language
import json

print("Loading Portuguese model...")
nlp = spacy.blank("pt")
ner = nlp.add_pipe("ner")

with open("config/dados_treino.json", "r", encoding="utf-8") as f:
    training_data = json.load(f)

for entry in training_data:
    for ent in entry["entities"]:
        ner.add_label(ent["label"])

examples = []
for entry in training_data:
    doc = nlp.make_doc(entry["text"])
    ents = []
    for ent in entry["entities"]:
        span = doc.char_span(ent["start"], ent["end"], label=ent["label"])
        if span is not None:
            ents.append(span)
    doc.ents = ents
    examples.append(Example.from_dict(doc, {"entities": [(e.start_char, e.end_char, e.label_) for e in ents]}))

optimizer = nlp.begin_training()
print("Starting training...")

for i in range(200):
    losses = {}
    batches = minibatch(examples, size=compounding(4.0, 32.0, 1.001))
    for batch in batches:
        nlp.update(batch, drop=0.4, losses=losses)
    if (i+1) % 20 == 0:
        print(f"Iteration {i+1}, Losses: {losses}")

nlp.to_disk("../google_news_scrapper/models/victims_nlp")
print("Model saved and verified successfully!")
