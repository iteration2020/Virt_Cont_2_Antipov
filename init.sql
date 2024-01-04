CREATE TABLE houseprices (
    id int primary key,
    property_type varchar(10),
    price numeric,
    location varchar(100),
    city varchar(50),
    baths int,
    purpose varchar(100),
    bedrooms int,
    Area_in_marla numeric
);

COPY houseprices(id, property_type, price, location, city, baths, purpose, bedrooms, Area_in_Marla)
FROM '/house_prices.csv' DELIMITER ',' CSV HEADER;