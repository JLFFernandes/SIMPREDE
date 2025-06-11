-- Extensões necessárias
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS postgis;

-- Tabela de desastres
CREATE TABLE IF NOT EXISTS public.disasters (
  id      SERIAL PRIMARY KEY,
  type    TEXT,
  subtype TEXT,
  date    DATE,
  year    INTEGER,
  month   INTEGER,
  day     INTEGER,
  hour    TEXT
  --CONSTRAINT unique_disaster_event UNIQUE (type, subtype, date, hour)
);

-- Tabela de localização
CREATE TABLE IF NOT EXISTS public.location (
  id            INTEGER PRIMARY KEY
                REFERENCES public.disasters(id),
  latitude      DOUBLE PRECISION NOT NULL,
  longitude     DOUBLE PRECISION NOT NULL,
  georef_class  INTEGER NOT NULL,
  district      TEXT,
  municipality  TEXT,
  parish        TEXT,
  dicofreg      TEXT,
  geom          geometry(Point,4326)
  --CONSTRAINT unique_location_coordinates UNIQUE (latitude, longitude, georef_class),
  --CONSTRAINT unique_location_dicofreg UNIQUE (dicofreg, georef_class)
);

-- Tabela de impactos humanos
CREATE TABLE IF NOT EXISTS public.human_impacts (
  id          INTEGER PRIMARY KEY
              REFERENCES public.disasters(id),
  fatalities  INTEGER,
  injured     INTEGER,
  evacuated   INTEGER,
  displaced   INTEGER,
  missing     INTEGER
);

-- Tabela de fontes de informação
CREATE TABLE IF NOT EXISTS public.information_sources (
  disaster_id  INTEGER
               REFERENCES public.disasters(id),
  source_name  TEXT,
  source_date  DATE,
  source_type  TEXT,
  page         TEXT
  --CONSTRAINT unique_source_per_disaster UNIQUE (disaster_id, source_name, source_date)
);
