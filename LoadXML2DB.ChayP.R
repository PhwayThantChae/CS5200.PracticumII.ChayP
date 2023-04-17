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
                    issn_type TEXT
                  )")

dbExecute(dbcon, "CREATE TABLE journals (
                    journal_id INTEGER PRIMARY KEY NOT NULL,
                    title TEXT,
                    issn_id INTEGER,
                    iso_abbreviation TEXT,
                    FOREIGN KEY (issn_id) REFERENCES issns(issn_id)
                  )")

dbExecute(dbcon, "CREATE TABLE articles (
                    pmid TEXT PRIMARY KEY NOT NULL,
                    title TEXT,
                    journal_id INTEGER NOT NULL,
                    FOREIGN KEY (journal_id) REFERENCES journals(journal_id)
                  )")

dbExecute(dbcon, "CREATE TABLE journal_issues (
                    journal_issue_id INTEGER PRIMARY KEY NOT NULL,
                    cited_medium TEXT,
                    volume INTEGER,
                    issue INTEGER,
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
                    author_id INTEGER PRIMARY KEY NOT NULL,
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
                    PRIMARY KEY (pmid, author_id),
                    FOREIGN KEY (pmid) REFERENCES articles(pmid),
                    FOREIGN KEY (author_id) REFERENCES authors(author_id)
                  )")


## 3. Extract and transform the data from the XML file
## Extract the Article nodes
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
                                journal_id = integer(),
                                stringsAsFactors = F)

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

get_issn_id <- function(issn_id, issn_type) {
  issn <- df.issn$issn_id[which(df.issn$issn_id == issn_id &
                              df.issn$issn_type == issn_type)]
  
  if (length(issn) <= 0) {
    return(issn)
  } else {
    issn <- df.issn$issn_id[which(df.issn$issn_id == "Unknown" &
                                    df.issn$issn_type == "Unknown")]
    return(issn)
  }
  
}

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
    
    df.journal[nrow(df.journal) + 1, ] <- list(journal_id, journal_title, 
                                               issn_id, iso_abbreviation)
    journal_id <- journal_id + 1
  }
  
  df.journal <- distinct(df.journal, title, issn_id, iso_abbreviation, .keep_all = TRUE)
  
  return(df.journal)
}

extract_author_data <- function() {
  authors <- xpathSApply(xmlDoc, author_list_path)
  author_id = 1
  
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
      
    
    df.author <- rbind(df.author, data.frame(author_id = author_id, 
                                             valid_yn = valid_yn,
                                             last_name = last_name,
                                             fore_name = fore_name,
                                             initials = initials,
                                             suffix = suffix,
                                             affiliation_info = affiliation_info,
                                             collective_name = collective_name,
                                             stringsAsFactors = F))
    author_id = author_id + 1
  }
  
  df.author <- distinct(df.author, valid_yn, last_name, fore_name, initials, suffix, affiliation_info, collective_name, .keep_all = TRUE)
  
  return(df.author)
}

#complete_yn <- xmlGetAttr(author, "CompleteYN")
#author_list <- xpathSApply(author, ".//Author")
#for(a in author_list) {
  
#}
#print("*************************8")

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
    end_date <- as.Date(paste(year, end_month_num, "28", sep = "-"))
    
    # Generate a sequence of dates that span the range
    date_range <- seq(start_date, end_date, by = "day")
    
    # Print the date range
    return(list(start_date = start_date, end_date = end_date))
  } else {
    return(list(start_date = "", end_date = ""))
  }
}

convert_month <- function(month) {
  months_abbrev <- c("jan", "feb", "mar", "apr", "may", "jun", 
                     "jul", "aug", "sep", "oct", "nov", "dec")
  months_int <- 1:12
  month_num <- match(tolower(month), months_abbrev)
  return(month_num)
}

# Define a function that takes a season name and returns the corresponding start month
season_to_month <- function(season_name) {
  seasons_table <- data.frame(season = c("winter", "spring", "summer", "fall"),
                              start_month = c(12, 3, 6, 9))
  
  season_name <- tolower(season_name)
  start_month <- seasons_table$start_month[seasons_table$season == season_name]
  return(start_month)
}

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
    
    journal_id <- df.journal$journal_id[which(df.journal$title == journal_title &
                                              df.journal$iso_abbreviation == iso_abbreviation &
                                              df.journal$issn_id == issn_id)]
    
    journal_issue <- xpathSApply(journals[[i]], ".//JournalIssue")[[1]]
    cited_medium <- xmlGetAttr(journal_issue, "CitedMedium")
    volume <- xpathSApply(journal_issue, "./Volume", xmlValue)
    issue <- xpathSApply(journal_issue, "./Issue", xmlValue)
    year <- xpathSApply(journal_issue, "./PubDate/Year", xmlValue)
    month <- xpathSApply(journal_issue, "./PubDate/Month", xmlValue)
    day <- as.numeric(xpathSApply(journal_issue, "./PubDate/Day", xmlValue))
    medline_date <- xpathSApply(journal_issue, "./PubDate/MedlineDate", xmlValue)
    season <- xpathSApply(journal_issue, "./PubDate/Season", xmlValue)
    
    if(is.null(day) || is.na(day) || (length(day) == 0)) {
      day <- 1
    }
    
    if(is.null(month) || is.na(month) || (length(month) == 0)) {
      month <- 1
    } else {
      month = convert_month(month)
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
    
    print(medline_date)
    print(published_date_str)
    print(season)
    
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
      issue = 0
    }
    
    df.journal_issue <- rbind(df.journal_issue, data.frame(journal_issue_id = journal_issue_id, 
                                                           cited_medium = cited_medium, 
                                                           volume = volume,
                                                           issue = issue,
                                                           published_year = as.numeric(year),
                                                           published_month = as.numeric(month),
                                                           published_day = as.numeric(day),
                                                           published_date = published_date_str,
                                                           medline_date = medline_date,
                                                           medline_start_date = medline_start_date,
                                                           medline_end_date = medline_end_date,
                                                           season = season,
                                                           journal_id = journal_id,
                                                           stringsAsFactors = F))
  
    journal_issue_id <- journal_issue_id + 1
    
  }
  
  df.journal_issue <- distinct(df.journal_issue, cited_medium, volume, issue, published_year, published_month, published_day, published_date, medline_date, medline_start_date,
                               medline_end_date, season, journal_id, .keep_all = TRUE)
  return(df.journal_issue)
}

# Extract the article nodes
articles <- getNodeSet(xmlDoc, "//Article")

# Extract the PMID and Article Title for each article
article_list <- lapply(articles, function(article) {
  pmid <- xmlGetAttr(article, "PMID")
  article_title <- xpathSApply(article, ".//ArticleTitle", xmlValue)
  #journal_id <- get_journal_id(article)
})


df.issn <- extract_issn_data()
df.journal <- extract_journal_data()
df.journal_issue <- extract_journal_issue_data()
df.author <- extract_author_data()

print(df.author)

## Write df to tables
#dbWriteTable(dbcon, "issns", df.issn, overwrite = T)
#dbWriteTable(dbcon, "journals", df.journal, overwrite = T)
#dbWriteTable(dbcon, "journal_issues", df.journal_issue, overwrite = T)