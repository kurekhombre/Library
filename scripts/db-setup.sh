#!/bin/sh

export PGUSER="postgres"

psql -c "CREATE DATABASE IF NOT EXISTS library"

psql -c "USE library"

psql -c "CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50),
    email VARCHAR(50)
)"
psql -c "
INSERT INTO users (username, email) VALUES ('user1', 'user1@example.com');
INSERT INTO users (username, email) VALUES ('user2', 'user2@example.com');
"