-- Tabela principal: disasters
CREATE TABLE disasters (
    id SERIAL PRIMARY KEY,
    type VARCHAR(100) NOT NULL,
    subtype VARCHAR(100),
    date DATE NOT NULL,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour VARCHAR(50)
);

-- Tabela location
CREATE TABLE location (
    id INTEGER PRIMARY KEY REFERENCES disasters(id),
    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7),
    georef_class INTEGER CHECK (georef_class BETWEEN 1 AND 5),
    district VARCHAR(100),
    municipality VARCHAR(100),
    parish VARCHAR(255),
    DICOFREG VARCHAR(20),
    geom geometry(POINT, 4326)
);

-- Tabela human_impacts
CREATE TABLE human_impacts (
    id INTEGER PRIMARY KEY REFERENCES disasters(id),
    fatalities INTEGER DEFAULT 0,
    injured INTEGER DEFAULT 0,
    evacuated INTEGER DEFAULT 0,
    displaced INTEGER DEFAULT 0,
    missing INTEGER DEFAULT 0
);

-- Tabela information_sources
CREATE TABLE information_sources (
    source_id SERIAL PRIMARY KEY,
    disaster_id INTEGER REFERENCES disasters(id),
    source_name VARCHAR(255),
    source_date DATE,
    source_type VARCHAR(50),
    page VARCHAR(20)
);

-- Índices
CREATE INDEX idx_location_geom ON location USING GIST (geom);
