import spacy

def fix_boundaries(text, entities):
    """
    Adjust entity boundaries to match token boundaries.
    """
    nlp = spacy.blank("pt")
    doc = nlp(text)
    fixed_entities = []
    
    for start, end, label in entities:
        # Find tokens that overlap with the entity span
        entity_tokens = [token for token in doc if token.idx <= start < token.idx + len(token.text) or 
                        token.idx < end <= token.idx + len(token.text)]
        
        if entity_tokens:
            # Adjust boundaries to token boundaries
            new_start = entity_tokens[0].idx
            new_end = entity_tokens[-1].idx + len(entity_tokens[-1].text)
            fixed_entities.append((new_start, new_end, label))
    
    return fixed_entities

if __name__ == "__main__":
    from train_victim_ner import TRAIN_DATA
    
    fixed_train_data = []
    for text, annotations in TRAIN_DATA:
        fixed_entities = fix_boundaries(text, annotations["entities"])
        fixed_train_data.append((text, {"entities": fixed_entities}))
    
    # Print the fixed training data
    print("TRAIN_DATA = [")
    for text, annotations in fixed_train_data:
        line = f'    ("{text}", {{"entities": {annotations["entities"]}}}),'
        print(line)
    print("]")
