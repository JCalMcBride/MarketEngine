drop table if exists item_statistics;
drop table if exists item_subtypes;
drop table if exists item_mod_ranks;
drop table if exists items;


CREATE TABLE items (
    id VARCHAR(255) PRIMARY KEY,
    item_name VARCHAR(255),
    item_type VARCHAR(255),
    url_name VARCHAR(255),
    thumb VARCHAR(255)
);

CREATE TABLE item_statistics (
    id VARCHAR(255) PRIMARY KEY,
    item_id VARCHAR(255),
    datetime TIMESTAMP,
    volume INT,
    min_price DECIMAL(10,2),
    max_price DECIMAL(10,2),
    avg_price DECIMAL(10,2),
    wa_price DECIMAL(10,2),
    median DECIMAL(10,2),
    order_type ENUM('Buy', 'Sell', 'Closed') DEFAULT NULL,
    subtype VARCHAR(255) DEFAULT NULL,
    moving_avg DECIMAL(10,2) DEFAULT NULL,
    open_price DECIMAL(10,2) DEFAULT NULL,
    closed_price DECIMAL(10,2) DEFAULT NULL,
    mod_rank INT DEFAULT NULL,
    donch_bot DECIMAL(10,2) DEFAULT NULL,
    donch_top DECIMAL(10,2) DEFAULT NULL
);

CREATE TABLE item_subtypes (
    item_id VARCHAR(255),
    sub_type VARCHAR(255),
    PRIMARY KEY (item_id, sub_type),
    FOREIGN KEY (item_id) REFERENCES items(id)
);

CREATE TABLE item_mod_ranks (
    item_id VARCHAR(255),
    mod_rank INT,
    PRIMARY KEY (item_id, mod_rank),
    FOREIGN KEY (item_id) REFERENCES items(id)  -- Adjust the referenced table and column name as needed
);