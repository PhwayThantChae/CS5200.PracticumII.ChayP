# Title: Part 2 - Create Star/Snowflake Schema
# Author: Phwe Thant Chay
# Date: 2023-04-19

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

#all_cons <- dbListConnections(MySQL())

#for(con in all_cons) {
  #dbDisconnect(con)
#}

mydbcon <-  dbConnect(MySQL(), user = db_user, password = db_password,
                   dbname = db_name, host = db_host, port = db_port)

## 2. Connect to SQlite Database
sqlitedbcon <- dbConnect(RSQLite::SQLite(), dbname = "pubmed-clean.sqlite")

## 3. Create Journal fact table in MySQL
dbExecute(mydbcon, "DROP TABLE IF EXISTS journal_facts")

dbExecute(mydbcon, "CREATE TABLE journal_facts (
                    journal_id INTEGER NOT NULL,
                    title TEXT,
                    year INTEGER,
                    quarter INTEGER,
                    month INTEGER,
                    articles_published INTEGER,
                    unique_authors INTEGER,
                    PRIMARY KEY (journal_id, year, quarter, month, articles_published, unique_authors)
                  )")


## 4. Get the number of articles published per journal per quarter and year
articles_per_journal_per_quarter_pub_date <- dbGetQuery(sqlitedbcon, "
  SELECT 
    journals.journal_id as journal_id,
    journals.title as title,
    journal_issues.published_year AS year,
    CAST((journal_issues.published_month - 1) / 3 + 1 AS INTEGER) AS quarter,
    journal_issues.published_month AS month,
    COUNT(DISTINCT articles.pmid) AS articles_published,
    COUNT(DISTINCT article_authors.author_id) AS unique_authors
  FROM 
    journals 
    JOIN journal_issues ON journals.journal_id = journal_issues.journal_id
    JOIN articles ON journal_issues.journal_issue_id = articles.journal_issue_id
    JOIN article_authors ON articles.pmid = article_authors.pmid
    JOIN authors ON authors.author_id = article_authors.author_id
  WHERE journal_issues.published_date <> '1700-01-01'
  GROUP BY 
    journals.journal_id,
    year,
    quarter,
    month
  ORDER BY
    journals.journal_id,
    year,
    quarter,
    month,
    articles_published,
    unique_authors
")


articles_per_journal_per_quarter_medline_end_date <- dbGetQuery(sqlitedbcon, "
  SELECT 
    journals.journal_id as journal_id,
    journals.title as title,
    SUBSTR(journal_issues.medline_end_date, 1, 4) AS year,
    CAST((SUBSTR(journal_issues.medline_end_date, 6, 2) - 1) / 3 + 1 AS INTEGER) AS quarter,
    SUBSTR(journal_issues.medline_end_date, 6, 2) AS month,
    COUNT(DISTINCT articles.pmid) AS articles_published,
    COUNT(DISTINCT article_authors.author_id) AS unique_authors
  FROM 
    journals 
    JOIN journal_issues ON journals.journal_id = journal_issues.journal_id
    JOIN articles ON journal_issues.journal_issue_id = articles.journal_issue_id
    JOIN article_authors ON articles.pmid = article_authors.pmid
    JOIN authors ON authors.author_id = article_authors.author_id
  WHERE journal_issues.published_date = '1700-01-01'
  GROUP BY 
    journals.journal_id,
    year,
    quarter,
    month
  ORDER BY
    journals.journal_id,
    year,
    quarter,
    month,
    articles_published,
    unique_authors
")

# 5. Write the result to journal_facts table in MySQL
dbWriteTable(mydbcon, name = "journal_facts", 
             value = articles_per_journal_per_quarter_pub_date, append = TRUE, 
             row.names = FALSE)

dbWriteTable(mydbcon, name = "journal_facts", 
             value = articles_per_journal_per_quarter_medline_end_date, append = TRUE, 
             row.names = FALSE)

# 6. Query the journal_facts MySQL table
journal_facts_data <- dbGetQuery(mydbcon, "SELECT * FROM journal_facts;" )
print(head(journal_facts_data))

# Disconnect from the database
dbDisconnect(sqlitedbcon)
dbDisconnect(mydbcon)