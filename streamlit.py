import streamlit as st
import psycopg2
import pandas as pd

# Function to connect to PostgreSQL database
def get_connection():
    return psycopg2.connect("dbname=simprede_db user=postgres password=yourpassword")

# Function to fetch complete disaster data
def get_disasters():
    conn = get_connection()
    query = """
        SELECT 
            d.id, d.type, d.subtype, d.date, d.year, d.month, d.day, d.hour,
            l.latitude, l.longitude, l.georef_class, l.district, l.municipality, l.parish, l.DICOFREG,
            h.fatalities, h.injured, h.evacuated, h.displaced, h.missing,
            STRING_AGG(DISTINCT i.source_name, ', ') AS source_names,
            STRING_AGG(DISTINCT i.source_date::text, ', ') AS source_dates,
            STRING_AGG(DISTINCT i.source_type, ', ') AS source_types,
            STRING_AGG(DISTINCT i.page, ', ') AS pages
        FROM disasters d
        LEFT JOIN location l ON d.id = l.id
        LEFT JOIN human_impacts h ON d.id = h.id
        LEFT JOIN information_sources i ON d.id = i.disaster_id
        GROUP BY 
            d.id, d.type, d.subtype, d.date, d.year, d.month, d.day, d.hour,
            l.latitude, l.longitude, l.georef_class, l.district, l.municipality, l.parish, l.DICOFREG,
            h.fatalities, h.injured, h.evacuated, h.displaced, h.missing
        ORDER BY d.date DESC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# UI start
st.title("🌍 Simprede Database Viewer")

# Show disaster data
st.subheader("Disaster Events")
df_disasters = get_disasters()
st.dataframe(df_disasters)

# Sidebar: Delete all data
st.sidebar.header("Database Management")

if st.sidebar.button("Reset Database (Delete All Data)"):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE information_sources, human_impacts, location, disasters RESTART IDENTITY CASCADE;")
    conn.commit()
    cur.close()
    conn.close()
    st.warning("All data has been deleted!")
    st.rerun()
