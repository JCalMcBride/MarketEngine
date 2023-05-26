create database if not exists market;
use market;

drop table if exists item_statistics;
drop table if exists item_subtypes;
drop table if exists item_mod_ranks;
drop table if exists items_in_set;
drop table if exists item_tags;
drop table if exists items;


CREATE TABLE items
(
    id        VARCHAR(255) PRIMARY KEY,
    item_name VARCHAR(255),
    item_type VARCHAR(255),
    url_name  VARCHAR(255),
    thumb     VARCHAR(255),
    max_rank  INT DEFAULT NULL
);

CREATE TABLE items_in_set
(
    item_id VARCHAR(255),
    set_id  VARCHAR(255),
    PRIMARY KEY (item_id, set_id),
    FOREIGN KEY (item_id) REFERENCES items (id),
    FOREIGN KEY (set_id) REFERENCES items (id)
);

CREATE TABLE item_tags
(
    item_id VARCHAR(255),
    tag     VARCHAR(255),
    PRIMARY KEY (item_id, tag),
    FOREIGN KEY (item_id) REFERENCES items (id)
);

CREATE TABLE item_subtypes
(
    item_id  VARCHAR(255),
    sub_type VARCHAR(255),
    PRIMARY KEY (item_id, sub_type),
    FOREIGN KEY (item_id) REFERENCES items (id)
);

CREATE TABLE item_statistics
(
    id           VARCHAR(255) PRIMARY KEY,
    item_id      VARCHAR(255),
    datetime     TIMESTAMP,
    volume       INT,
    min_price    DECIMAL(10, 2),
    max_price    DECIMAL(10, 2),
    avg_price    DECIMAL(10, 2),
    wa_price     DECIMAL(10, 2),
    median       DECIMAL(10, 2),
    order_type   ENUM ('Buy', 'Sell', 'Closed') DEFAULT NULL,
    subtype      VARCHAR(255)                   DEFAULT NULL,
    moving_avg   DECIMAL(10, 2)                 DEFAULT NULL,
    open_price   DECIMAL(10, 2)                 DEFAULT NULL,
    closed_price DECIMAL(10, 2)                 DEFAULT NULL,
    mod_rank     INT                            DEFAULT NULL,
    donch_bot    DECIMAL(10, 2)                 DEFAULT NULL,
    donch_top    DECIMAL(10, 2)                 DEFAULT NULL
);

CREATE TABLE item_aliases (
    item_id VARCHAR(255),
    alias VARCHAR(255),
    PRIMARY KEY (item_id, alias),
    FOREIGN KEY (item_id) REFERENCES items (id)
);

CREATE TABLE word_aliases (
    word VARCHAR(255),
    alias VARCHAR(255),
    PRIMARY KEY (word, alias)
);