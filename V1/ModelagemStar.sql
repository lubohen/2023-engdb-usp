CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    week INT NOT NULL,
    day INT NOT NULL
);

CREATE TABLE dim_location (
    location_id SERIAL PRIMARY KEY,
    county VARCHAR(255) NOT NULL,
    state VARCHAR(255) NOT NULL
);

CREATE TABLE facts_covid (
    date_id INT,
    location_id INT,
    cases INT,
    deaths INT,
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id)
);
