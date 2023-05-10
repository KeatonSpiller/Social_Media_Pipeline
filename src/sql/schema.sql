-- TINYINT = 1 byte (8 bit) -- SMALLINT = 2 bytes (16 bit) -- MEDIUMINT = 3 bytes (24 bit)
-- INT = 4 bytes (32 bit) -- BIGINT = 8 bytes (64 bit)
-- SHOW DATABASES;

-- Simple db Schema
-- DROP DATABASE IF EXISTS db;
-- CREATE DATABASE db;

-- -- db Clients Table
-- DROP TABLE IF EXISTS db.clients;
-- CREATE TABLE  db.clients (
--     id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
--     firstname VARCHAR(100) NOT NULL,
--     lastname VARCHAR(100) NOT NULL,
--     email VARCHAR(200) NOT NULL,
--     reservation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
-- )

CREATE TABLE IF NOT EXISTS RawTweets (
    id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    entry_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    twitter_id INT NOT NULL,
    user VARCHAR(100) NOT NULL,
    group VARCHAR(100) NOT NULL,
    tweet_url VARCHAR(200) NOT NULL,
    favorite_count INT NOT NULL,
    retweet_count INT NOT NULL,
    hashtags VARCHAR(200) NOT NULL,
    emojis VARCHAR(200) NOT NULL,
    emoji_text VARCHAR(200) NOT NULL,
    usernames VARCHAR(200) NOT NULL,
    links VARCHAR(200) NOT NULL,
    tweet VARCHAR(200) NOT NULL);

-- INSERT INTO db.clients (firstname, lastname, email)
-- VALUES ('bill','Gates','bill.gates@microsoft.com');

