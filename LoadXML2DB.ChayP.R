# Author: Phwe Thant Chay
# Date: 2023-04-19
# Title: Part 1 - Load XML Data into Database

runtime <- system.time({
library(XML)
library(RSQLite)
library(dplyr)
library(stringr)

## 1. Load XML file
#xmlFile <- "pubmed-tfm-xml/pubmed22n0001-tf.xml"
#xmlFile <- "http://s3.amazonaws.com/cs5200.practicum2.chayp.xml/pubmed22n0001-tf.xml"
xmlFile <- "pubmed-tfm-xml/test.xml"
xmlDoc <- xmlParse(xmlFile, validate = TRUE)

## 2. Create table schema
dbcon <- dbConnect(RSQLite::SQLite(), dbname = "pubmed.sqlite")

dbExecute(dbcon, "DROP TABLE IF EXISTS articles")
dbExecute(dbcon, "DROP TABLE IF EXISTS issns")
dbExecute(dbcon, "DROP TABLE IF EXISTS journals")
dbExecute(dbcon, "DROP TABLE IF EXISTS journal_issues")
dbExecute(dbcon, "DROP TABLE IF EXISTS authors")
dbExecute(dbcon, "DROP TABLE IF EXISTS article_authors")


dbExecute(dbcon, "CREATE TABLE issns (
                    issn_id TEXT,
                    issn_type TEXT,
                    PRIMARY KEY (issn_id)
                  )")

dbExecute(dbcon, "CREATE TABLE journals (
                    journal_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    title TEXT,
                    issn_id INTEGER,
                    iso_abbreviation TEXT,
                    FOREIGN KEY (issn_id) REFERENCES issns(issn_id)
                  )")

dbExecute(dbcon, "CREATE TABLE articles (
                    pmid TEXT NOT NULL,
                    title TEXT,
                    journal_id INTEGER NOT NULL,
                    journal_issue_id INTEGER NOT NULL,
                    PRIMARY KEY (pmid),
                    FOREIGN KEY (journal_id) REFERENCES journals(journal_id),
                    FOREIGN KEY (journal_issue_id) REFERENCES journal_issues(journal_issue_id)
                  )")

dbExecute(dbcon, "CREATE TABLE journal_issues (
                    journal_issue_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    cited_medium TEXT,
                    volume INTEGER,
                    issue TEXT,
                    published_year INTEGER,
                    published_month INTEGER,
                    published_day INTEGER,
                    published_date DATE,
                    medline_date TEXT,
                    medline_start_date DATE,
                    medline_end_date DATE,
                    season TEXT,
                    journal_id INTEGER,
                    FOREIGN KEY (journal_id) REFERENCES journals(journal_id)
                  )")

dbExecute(dbcon, "CREATE TABLE authors (
                    author_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    last_name TEXT,
                    fore_name TEXT,
                    initials TEXT,
                    valid_yn TEXT,
                    suffix TEXT,
                    collective_name TEXT,
                    affiliation_info TEXT
                  )")

dbExecute(dbcon, "CREATE TABLE article_authors (
                    author_id INTEGER,
                    pmid TEXT,
                    complete_yn TEXT,
                    PRIMARY KEY (pmid, author_id),
                    FOREIGN KEY (pmid) REFERENCES articles(pmid),
                    FOREIGN KEY (author_id) REFERENCES authors(author_id)
                  )")


## 3. Extract and transform the data from the XML file
article_path <- "//Article"
issn_path <- "//Article/PubDetails/Journal/ISSN"
journal_path <- "//Article/PubDetails/Journal"
author_list_path <- "//Article/PubDetails/AuthorList/Author"

df.issn <- data.frame ( issn_id = character(),
                        issn_type = character(),
                        stringsAsFactors = F)


df.journal <- data.frame ( journal_id = integer(0),
                           title = character(),
                           issn_id = character(),
                           iso_abbreviation = character(),
                           stringsAsFactors = F)

df.article <- data.frame ( pmid = character(0),
                           article_title = character(),
                           journal_id = integer(0),
                           journal_issue_id = integer(0),
                           stringsAsFactors = F)

df.author <- data.frame(author_id = integer(0),
                        valid_yn = character(0),
                        last_name = character(0),
                        fore_name = character(0),
                        initials = character(0),
                        suffix = character(0),
                        affiliation_info = character(0),
                        collective_name = character(0))

df.journal_issue <- data.frame( journal_issue_id = integer(0),
                                cited_medium = character(),
                                volume = integer(),
                                issue = integer(),
                                published_year = integer(),
                                published_month = integer(),
                                published_day = integer(),
                                published_date = character(),
                                medline_date = character(),
                                medline_start_date = character(),
                                medline_end_date = character(),
                                season = character(),
                                journal_id = integer(0),
                                stringsAsFactors = F)

df.article_author <- data.frame(author_id = integer(0),
                                pmid = character(),
                                complete_yn = character())

## Function to get ISSN data from XML
extract_issn_data <- function() {

  issn_ids <- xpathSApply(xmlDoc, issn_path)
  
  for(issn in issn_ids) {
    issn_type <- xmlGetAttr(issn, "IssnType")
    issn_id <- xpathSApply(issn, ".", xmlValue)
    df.issn <- rbind(df.issn, data.frame(issn_id = issn_id, 
                                         issn_type = issn_type, 
                                         stringsAsFactors = F))
  }
  
  df.issn <- rbind(df.issn, data.frame(issn_id = "Unknown", 
                                       issn_type = "Unknown", 
                                       stringsAsFactors = F))
  
  df.issn <- distinct(df.issn, issn_id, issn_type, .keep_all = TRUE)
  return(df.issn)
}

## Function to get author data from XML
extract_author_data <- function() {
  authors <- xpathSApply(xmlDoc, author_list_path)
  author_id = 1
  df_list <- vector(mode = "list", length = length(authors))
  
  for(author in authors) {
    valid_yn <- xmlGetAttr(author, "ValidYN")
    last_name <- xpathSApply(author, "./LastName", xmlValue)
    fore_name <- xpathSApply(author, "./ForeName", xmlValue)
    initials <- xpathSApply(author, "./Initials", xmlValue)
    suffix <- xpathSApply(author, "./Suffix", xmlValue)
    affiliation_info <- xpathSApply(author, "./AffiliationInfo/Affiliation", xmlValue)
    collective_name <- xpathSApply(author, "./CollectiveName", xmlValue)
    
    if(length(valid_yn) == 0) {
      valid_yn <- ""
    }
    
    if(length(last_name) == 0) {
      last_name <- ""
    }
    
    if(length(fore_name) == 0) {
      fore_name <- ""
    }
    
    if(length(initials) == 0) {
      initials <- ""
    }
    
    if(length(suffix) == 0) {
      suffix <- ""
    }
    
    if(length(affiliation_info) == 0) {
      affiliation_info <- ""
    }
    
    if(length(collective_name) == 0) {
      collective_name <- ""
    }
    
    df_list[[author_id]] <- data.frame(author_id = as.integer(author_id), 
                               valid_yn = valid_yn,
                               last_name = last_name,
                               fore_name = fore_name,
                               initials = initials,
                               suffix = suffix,
                               affiliation_info = affiliation_info,
                               collective_name = collective_name,
                               stringsAsFactors = FALSE)
      
    author_id = author_id + 1
  }
  df.author <- do.call(rbind, df_list)
  df.author <- distinct(df.author, valid_yn, last_name, fore_name, initials, suffix, affiliation_info, collective_name, .keep_all = TRUE)
  
  return(df.author)
}

## Function to convert medline date to start date and end date
convert_medline_daterange <- function(medline_date) {
  
  if (grepl("\\d{4} [[:alpha:]]{3}-[[:alpha:]]{3}", medline_date)) {
    # Extract the year and month names from the string using regular expressions
    year <- str_extract(medline_date, "\\d{4}")
    start_month <- tolower(str_extract(medline_date, "[[:alpha:]]{3}"))
    end_month <- tolower(str_extract_all(medline_date, "[[:alpha:]]{3}")[[1]][2])
    
    # Convert the month names to numeric values
    start_month_num <- convert_month(start_month)
    end_month_num <- convert_month(end_month)
    
    
    # Create a start date for the range
    start_date <- as.Date(paste(year, start_month_num, "01", sep = "-"))
    
    # Create an end date for the range
    end_date <- as.Date(paste(year, end_month_num, "01", sep = "-"))
    
    # Generate a sequence of dates that span the range
    date_range <- seq(start_date, end_date, by = "day")
    
  } else if (grepl("\\d{4} [[:alpha:]]{3}-\\d{4} (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)", medline_date)) {
    matches <- regmatches(medline_date, regexpr("\\d{4} [A-Za-z]{3}-\\d{4} [A-Za-z]{3}", medline_date))
    if (length(matches) > 0) {
      date_parts <- strsplit(matches, "-| ")
      start_month <- convert_month(date_parts[[1]][2])
      end_month <- convert_month(date_parts[[1]][4])
      
      start_date <- as.Date(paste(date_parts[[1]][1], start_month, "01", sep = "-"), "%Y-%m-%d")
      end_date <- as.Date(paste(date_parts[[1]][3], end_month + 1, "01", sep = "-"), "%Y-%m-%d") - 1
    }
  } else if(grepl("\\d{4}-\\d{4}", medline_date)) {
    matches <- regmatches(medline_date, regexpr("\\d{4}-\\d{4}", medline_date))
    if (length(matches) > 0) {
      date_parts <- strsplit(matches, "-| ")
      start_year <- date_parts[[1]][1]
      end_year <- date_parts[[1]][2]
      
      start_date <- as.Date(paste(start_year, "12", "01", sep = "-"), "%Y-%m-%d")
      end_date <- as.Date(paste(end_year, "12", "01", sep = "-"), "%Y-%m-%d")
    }
  } else if(grepl("\\d{4} [[:alpha:]]{3} [[:digit:]]{1,2}-[[:digit:]]{1,2}", medline_date)) {
    year <- str_extract(medline_date, "\\d{4}")
    start_month <- tolower(str_extract(medline_date, "[[:alpha:]]{3}"))
    start_day <- str_extract(medline_date, "\\d{1,2}(?=-)")
    end_day <- str_extract(medline_date, "(?<=-)\\d{1,2}")
    
    # Convert the start month name to a numeric value
    start_month_num <- convert_month(start_month)
    
    # Create a start date
    start_date <- as.Date(paste(year, start_month_num, start_day, sep = "-"))
    
    # Create an end date
    end_date <- as.Date(paste(year, start_month_num, end_day, sep = "-"))
  } else if(grepl("\\d{4} [[:alpha:]]{3} [[:digit:]]{1,2}-[[:alpha:]]{3} [[:digit:]]{1,2}", medline_date)) {
    # 1976 Aug 28-Sep 4
    date_parts <- strsplit(medline_date, "-")
    
    # Extract the year from the first part of the date range
    year <- substr(date_parts[[1]], 1, 4)[1]
    
    # Extract the month and day from the first part of the date range
    start_month <- substr(date_parts[[1]], 6, 8)[1]
    start_day <- substr(date_parts[[1]], 10, 11)[1]
    
    # Extract the month and day from the second part of the date range
    end_month <- substr(date_parts[[1]], 1, 3)[2]
    end_day <- substr(date_parts[[1]], 5, 6)[2]
    
    # Convert the month names to numeric values
    start_month_num <- convert_month(start_month)
    end_month_num <- convert_month(end_month)
    
    # Create the start date and end date using the extracted information
    start_date <- as.Date(paste(year, start_month_num, start_day, sep = "-"))
    end_date <- as.Date(paste(year, end_month_num, end_day, sep = "-"))
  }
  
  return(list(start_date = start_date, end_date = end_date))
}

## Function to convert month string to integer
convert_month <- function(month) {
  months_abbrev <- c("jan", "feb", "mar", "apr", "may", "jun", 
                     "jul", "aug", "sep", "oct", "nov", "dec")
  months_int <- 1:12
  month_num <- match(tolower(month), months_abbrev)
  return(month_num)
}

## Function to convert season to month integer
season_to_month <- function(season_name) {
  seasons_table <- data.frame(season = c("winter", "spring", "summer", "fall"),
                              start_month = c(12, 3, 6, 9))
  
  season_name <- tolower(season_name)
  start_month <- seasons_table$start_month[seasons_table$season == season_name]
  return(start_month)
}

## Function to extract journal issue from journal
get_journal_issue_from_journal <- function(journal) {
  journal_issue <- xpathSApply(journal, ".//JournalIssue")[[1]]
  cited_medium <- xmlGetAttr(journal_issue, "CitedMedium")
  volume <- xpathSApply(journal_issue, "./Volume", xmlValue)
  issue <- xpathSApply(journal_issue, "./Issue", xmlValue)
  year <- xpathSApply(journal_issue, "./PubDate/Year", xmlValue)
  month <- xpathSApply(journal_issue, "./PubDate/Month", xmlValue)
  day <- as.integer(xpathSApply(journal_issue, "./PubDate/Day", xmlValue))
  medline_date <- xpathSApply(journal_issue, "./PubDate/MedlineDate", xmlValue)
  season <- xpathSApply(journal_issue, "./PubDate/Season", xmlValue)
  
  if(is.null(day) || is.na(day) || (length(day) == 0)) {
    day <- 1
  }
  
  if(is.null(month) || is.na(month) || (length(month) == 0)) {
    month <- 1
  } else {
    if(month %in% month.abb) {
      month = convert_month(month)
    } else {
      month = as.integer(month) 
    }
  }
  
  if(is.null(year) || is.na(year) || (length(year) == 0)) {
    year <- 0
  }
  
  if(year == 0) {
    year = 1700
  }
  
  if(year != 0 && !is.null(season) && !is.na(season) && (length(season) != 0)) {
    month = season_to_month(season)
  }
  
  published_date = paste(year, month, day, sep = "-")
  published_date_obj <- as.Date(published_date)
  published_date_str <- format(published_date_obj, "%Y-%m-%d")
  
  medline_start_date = ""
  medline_end_date = ""
  
  if(!is.null(medline_date) && !is.na(medline_date) && (length(medline_date) != 0)) {
    medline_date_range = convert_medline_daterange(medline_date)
    medline_start_date = format(medline_date_range$start_date, "%Y-%m-%d")
    medline_end_date = format(medline_date_range$end_date, "%Y-%m-%d")
    
  } else {
    medline_date = ""
  }
  
  if(is.null(season) || is.na(season) || (length(season) == 0)) {
    season = ""
  }
  
  if(is.null(volume) || is.na(volume) || (length(volume) == 0)) {
    volume = 0
  }
  
  if(is.null(issue) || is.na(issue) || (length(issue) == 0)) {
    issue = ""
  }
  
  journal_issue_data <- list()
  journal_issue_data$cited_medium <- cited_medium
  journal_issue_data$volume <- volume
  journal_issue_data$issue <- issue
  journal_issue_data$year <- as.integer(year)
  journal_issue_data$month <- as.integer(month)
  journal_issue_data$day <- as.integer(day)
  journal_issue_data$published_date <- published_date_str
  journal_issue_data$medline_date <- medline_date
  journal_issue_data$medline_start_date <- medline_start_date
  journal_issue_data$medline_end_date <- medline_end_date
  journal_issue_data$season <- season
  
  return(journal_issue_data)
}

## Function to extract journal issue data from XML
extract_journal_issue_data <- function() {
  journals <- xpathApply(xmlDoc, journal_path)
  journal_issue_id = 1
  
  for(i in 1:length(journals)) {
    
    issn_id <- xpathSApply(journals[[i]], "./ISSN", xmlValue)
    if(length(issn_id) == 0 || is.na(issn_id)) {
      issn_id = "Unknown"
    }
    journal_title = xpathSApply(journals[[i]], "./Title", xmlValue)
    iso_abbreviation = xpathSApply(journals[[i]], ".//ISOAbbreviation", xmlValue)
    
    journal_id <- df.journal[df.journal$title == journal_title &
                               df.journal$iso_abbreviation == iso_abbreviation &
                               df.journal$issn_id == issn_id, ]$journal_id

    journal_issue = get_journal_issue_from_journal(journals[[i]])
    
    df.journal_issue[nrow(df.journal_issue) + 1, ] <- list(as.integer(journal_issue_id), 
                                                           journal_issue$cited_medium,
                                                           journal_issue$volume,
                                                           journal_issue$issue,
                                                           journal_issue$year,
                                                           journal_issue$month,
                                                           journal_issue$day,
                                                           journal_issue$published_date,
                                                           journal_issue$medline_date,
                                                           journal_issue$medline_start_date,
                                                           journal_issue$medline_end_date,
                                                           journal_issue$season,
                                                           as.integer(journal_id))
  
    journal_issue_id <- journal_issue_id + 1
    
  }
  
  df.journal_issue <- distinct(df.journal_issue, cited_medium, volume, issue, published_year, published_month, published_day, published_date, medline_date, medline_start_date,
                               medline_end_date, season, journal_id, .keep_all = TRUE)
  return(df.journal_issue)
}

## Function to extract journal data from XML
extract_journal_data <- function() {
  journals <- xpathSApply(xmlDoc, journal_path)
  journal_id <- 1
  
  for(journal in journals) {
    issn_id <- xpathSApply(journal, ".//ISSN", xmlValue)
    if(length(issn_id) == 0 || is.na(issn_id)) {
      issn_id = "Unknown"
    }
    journal_title = xpathSApply(journal, ".//Title", xmlValue)
    iso_abbreviation = xpathSApply(journal, ".//ISOAbbreviation", xmlValue)
    
    df.journal[nrow(df.journal) + 1, ] <- list(as.integer(journal_id), journal_title, 
                                               issn_id, iso_abbreviation)
    journal_id <- journal_id + 1
  }
  
  df.journal <- distinct(df.journal, title, issn_id, iso_abbreviation, .keep_all = TRUE)
  
  return(df.journal)
}

## Function to extract article data from XML
extract_article_data <- function() {
  articles <- xpathApply(xmlDoc, article_path)
  for(article in articles) {
    pmid <- xmlGetAttr(article, "PMID")
    article_title <- xpathSApply(article, ".//ArticleTitle", xmlValue)
    journal <- xpathSApply(article, "./PubDetails/Journal")[[1]]
    
    ## Get journal_id for article
    issn_id <- xpathSApply(journal, ".//ISSN", xmlValue)
    if(length(issn_id) == 0 || is.na(issn_id)) {
      issn_id = "Unknown"
    }
    journal_title = xpathSApply(journal, ".//Title", xmlValue)
    iso_abbreviation = xpathSApply(journal, ".//ISOAbbreviation", xmlValue)
    
    
    journal_id <- df.journal[df.journal$title == journal_title &
                             df.journal$iso_abbreviation == iso_abbreviation &
                             df.journal$issn_id == issn_id, ]$journal_id

    journal_issue = get_journal_issue_from_journal(journal)
    
    journal_issue_id <- df.journal_issue[df.journal_issue$cited_medium == journal_issue$cited_medium & 
                                           df.journal_issue$volume == journal_issue$volume &                                  
                                           df.journal_issue$issue == journal_issue$issue &                                  
                                           df.journal_issue$published_date == journal_issue$published_date &
                                           df.journal_issue$medline_date == journal_issue$medline_date &
                                           df.journal_issue$medline_start_date == journal_issue$medline_start_date &
                                           df.journal_issue$medline_end_date == journal_issue$medline_end_date &
                                           df.journal_issue$season == journal_issue$season &
                                           df.journal_issue$journal_id == journal_id, ]$journal_issue_id
    
    df.article[nrow(df.article) + 1, ] <- list(pmid, article_title, as.integer(journal_id), as.integer(journal_issue_id))
  }
  
  df.article <- distinct(df.article, article_title, journal_id, .keep_all = TRUE)
  
  return(df.article)
}

## Function to get article author junction data from XML
extract_article_author_data <- function() {
  articles <- xpathApply(xmlDoc, article_path)

  for(article in articles) {
    pmid <- xmlGetAttr(article, "PMID")
    authorLists <- xpathSApply(article, ".//AuthorList")
    for (authorList in authorLists) {
      complete_yn <- xmlGetAttr(authorList, "CompleteYN")
      if(is.null(complete_yn) || is.na(complete_yn) || (length(complete_yn) == 0)) {
        complete_yn = ""
      }
      authors <- xpathSApply(authorList, ".//Author")
      for (author in authors) {
        valid_yn <- xmlGetAttr(author, "ValidYN")
        last_name <- xpathSApply(author, "./LastName", xmlValue)
        fore_name <- xpathSApply(author, "./ForeName", xmlValue)
        initials <- xpathSApply(author, "./Initials", xmlValue)
        suffix <- xpathSApply(author, "./Suffix", xmlValue)
        affiliation_info <- xpathSApply(author, "./AffiliationInfo/Affiliation", xmlValue)
        collective_name <- xpathSApply(author, "./CollectiveName", xmlValue)
        
        if(length(valid_yn) == 0) {
          valid_yn <- ""
        }
        
        if(length(last_name) == 0) {
          last_name <- ""
        }
        
        if(length(fore_name) == 0) {
          fore_name <- ""
        }
        
        if(length(initials) == 0) {
          initials <- ""
        }
        
        if(length(suffix) == 0) {
          suffix <- ""
        }
        
        if(length(affiliation_info) == 0) {
          affiliation_info <- ""
        }
        
        if(length(collective_name) == 0) {
          collective_name <- ""
        }
        
        author_id <- df.author[df.author$last_name == last_name & 
                                   df.author$fore_name == fore_name &                                  
                                   df.author$initials == initials &                                  
                                   df.author$suffix == suffix &                                  
                                   df.author$affiliation_info == affiliation_info &                                  
                                   df.author$collective_name == collective_name, ]$author_id
        
        
        #author_id <- df.author$author_id[which(df.author$last_name == last_name &
                                               #df.author$fore_name == fore_name &
                                               #df.author$initials == initials &
                                               #df.author$suffix == suffix &
                                               #df.author$affiliation_info == affiliation_info &
                                               #df.author$collective_name == collective_name) ]
        
        #df.article_author <- rbind(df.article_author, data.frame(pmid = pmid, 
                                                                 #author_id = as.integer(author_id),
                                                                 #complete_yn = complete_yn,
                                                                 #stringsAsFactors = F))
        
        df.article_author[nrow(df.article_author) + 1, ] <- list(author_id, pmid, 
                                                                 complete_yn)
        
      }
    }
  }

  df.article_author <- distinct(df.article_author, pmid, author_id, complete_yn, .keep_all = TRUE)
  
  return(df.article_author)
}

## Main script
print("Extracting ISSN Data")
print("---------------------")
df.issn <- extract_issn_data()
print("Extracting Journal Data")
print("---------------------")
df.journal <- extract_journal_data()
print("Extracting Journal Issue Data")
print("---------------------")
df.journal_issue <- extract_journal_issue_data()
print("Extracting Author Data")
print("---------------------")
df.author <- extract_author_data()
print("Extracting Article Data")
print("---------------------")
df.article <- extract_article_data()
print("Extracting Article Author Data")
print("---------------------")
df.article_author <- extract_article_author_data()


## Write extracted dataframes to sqlite tables
dbWriteTable(dbcon, "issns", df.issn, overwrite = T)
dbWriteTable(dbcon, "journals", df.journal, overwrite = T)
dbWriteTable(dbcon, "journal_issues", df.journal_issue, overwrite = T)
dbWriteTable(dbcon, "authors", df.author, overwrite = T)
dbWriteTable(dbcon, "articles", df.article, overwrite = T)
dbWriteTable(dbcon, "article_authors", df.article_author, overwrite = T)
})

cat(sprintf("Total runtime: %.2f seconds\n", runtime[[3]]))