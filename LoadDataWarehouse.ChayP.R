# Author: Phwe Thant Chay
# Date: 2023-04-19
# Title: Part 2 - Create Star/Snowflake Schema

library(RMySQL)
library(sqldf)
library(dplyr)
library(RSQLite)

options(sqldf.driver = 'SQLite')

## 1. Connect to MySQL Database
db_user <- 'root'
db_password <- '12345678'
db_name <- 'pubmed_snowflake'
db_host <- 'localhost'
db_port <- 3306

mydbcon <-  dbConnect(MySQL(), user = db_user, password = db_password,
                   dbname = db_name, host = db_host, port = db_port)

## 2. Connect to Sqlite Database
sqlitedbcon <- dbConnect(RSQLite::SQLite(), dbname = "pubmed-clean.sqlite")

## 3. Create Journal fact table in MySQL
dbExecute(dbcon, "CREATE TABLE journal_facts (
                    journal_id INTEGER NOT NULL,
                    title TEXT,
                    year INTEGER,
                    quarter INTEGER,
                    articles_published INTEGER,
                    unique_authors INTEGER,
                    PRIMARY KEY (journal_id, year, quarter),
                    FOREIGN KEY (journal_id) REFERENCES journals(journal_id)
                  )")