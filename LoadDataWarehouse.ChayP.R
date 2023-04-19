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
dbExecute(dbcon, "DROP TABLE IF EXISTS journal_facts")

dbExecute(dbcon, "CREATE TABLE journal_facts (
                    journal_id INTEGER NOT NULL,
                    title TEXT,
                    year INTEGER,
                    quarter INTEGER,
                    articles_published INTEGER,
                    unique_authors INTEGER,
                    PRIMARY KEY (journal_id, year, quarter)
                  )")


# 4. Get the number of articles published per journal per quarter and year
articles_per_journal_per_quarter <- dbGetQuery(sqlitedbcon, "
  SELECT 
    journals.journal_id,
    strftime('%Y', journal_issues.published_date) AS year,
    CAST((strftime('%m', journal_issues.published_date) - 1) / 3 + 1 AS INTEGER) AS quarter,
    COUNT(DISTINCT articles.pmid) AS num_articles,
    COUNT(DISTINCT article_authors.author_id) AS num_unique_authors
  FROM 
    journals 
    JOIN journal_issues ON journals.journal_id = journal_issues.journal_id
    JOIN articles ON journals.journal_id = articles.journal_id
    JOIN article_authors ON articles.pmid = article_authors.pmid
  GROUP BY 
    journals.journal_id,
    year,
    quarter
  ORDER BY
    journals.journal_id,
    year,
    quarter,
    num_articles,
    num_unique_authors
")

articles_per_journal_per_quarter