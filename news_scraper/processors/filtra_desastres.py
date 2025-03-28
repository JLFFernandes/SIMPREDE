import pandas as pd
from utils.helpers import (
    normalize,
    extract_title_from_text,
    extract_victim_counts,
    detect_disaster_type,
    verificar_localizacao,
    is_in_portugal,
    is_potentially_disaster_related,
    extract_event_hour,
    parse_event_date
)

INPUT_CSV = "data/artigos_brutos.csv"
OUTPUT_CSV_VALIDOS = "data/artigos_filtrados.csv"
OUTPUT_CSV_DESCARTADOS = "data/artigos_descartados.csv"


def filtrar_e_transformar():
    df = pd.read_csv(INPUT_CSV)
    artigos_validos = []
    artigos_descartados = []

    for _, row in df.iterrows():
        texto = row.get("fulltext", "")
        if not texto or len(texto) < 100:
            row["motivo"] = "texto curto"
            artigos_descartados.append(row)
            continue

        titulo = extract_title_from_text(texto) or row.get("title", "")

        if not is_in_portugal(titulo, texto):
            row["motivo"] = "fora de Portugal"
            artigos_descartados.append(row)
            continue

        if not is_potentially_disaster_related(texto):
            row["motivo"] = "sem relação com desastre"
            artigos_descartados.append(row)
            continue

        localizacao = verificar_localizacao(texto)
        if not localizacao:
            row["motivo"] = "município não identificado"
            artigos_descartados.append(row)
            continue

        tipo, subtipo = detect_disaster_type(texto)
        vitimas = extract_victim_counts(texto)
        hora_evento = extract_event_hour(texto)
        data_str = row.get("date")
        _, ano, mes, dia = parse_event_date(data_str)

        artigo = {
            "id": row.get("ID"),
            "type": tipo,
            "subtype": subtipo,
            "date": data_str,
            "year": ano,
            "month": mes,
            "day": dia,
            "hour": hora_evento,
            "georef": localizacao["georef"],
            "district": localizacao["district"],
            "municipali": localizacao["municipali"],
            "parish": localizacao["parish"],
            "dicofreg": localizacao["dicofreg"],
            "source": row.get("source"),
            "sourcedate": data_str,
            "sourcetype": row.get("sourcetype"),
            "page": None,
            **vitimas,
            "geom": None
        }
        artigos_validos.append(artigo)

    if artigos_validos:
        colunas_ordenadas = [
            "id", "type", "subtype", "date", "year", "month", "day", "hour",
            "georef", "district", "municipali", "parish", "dicofreg",
            "source", "sourcedate", "sourcetype", "page",
            "fatalities", "injured", "evacuated", "displaced", "missing", "geom"
        ]
        pd.DataFrame(artigos_validos)[colunas_ordenadas].to_csv(OUTPUT_CSV_VALIDOS, index=False)
        print(f"✅ {len(artigos_validos)} artigos válidos guardados em {OUTPUT_CSV_VALIDOS}")

    if artigos_descartados:
        pd.DataFrame(artigos_descartados).to_csv(OUTPUT_CSV_DESCARTADOS, index=False)
        print(f"⚠️ {len(artigos_descartados)} artigos descartados guardados em {OUTPUT_CSV_DESCARTADOS}")


if __name__ == "__main__":
    filtrar_e_transformar()
