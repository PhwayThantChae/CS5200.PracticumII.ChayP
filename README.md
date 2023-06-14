## CS5200 Practicum II

### Part 1: Load XML Data into Database

<ol>
    <li>Create an R Script called <b>LoadXML2DB.LastNameF.R</b> for Part 1.</li>
    <li>Download the XML file pubmed22n0001-tf.xml and save it in the pubmed-tfm-xml subfolder of your project folder.</li>
    <li>Build an external DTD for the XML file and reference it from the downloaded XML.</li>
    <li>Design a normalized relational schema with the following entities: Articles, Journals, Authors. Determine the appropriate attributes for each entity based on the  XML document. Include primary and foreign keys, and add synthetic surrogate keys where necessary.</li>
    <li>Realize the relational schema in SQLite using CREATE TABLE statements in R.</li>
    <li>Load the XML file into R from a URL or locally, ensuring validation of the DTD.</li>
    <li>Extract and transform the data from the XML file and populate the appropriate tables in the database.</li>
</ol>



### Part 2: Create Star/Snowflake Schema

<ol>
    <li>Create a new R Script called <b>LoadDataWarehouse.LastNameF.R</b> for Part 2.</li>
    <li>Create a MySQL database and establish a connection.</li>
    <li>Design and populate a star schema for journal facts. The fact table should include the journal PK, title, number of articles published per quarter and per year, and number of unique authors published per quarter and per month. Use the data from the SQLite database created in Part 1 and populate the fact table via R. Ensure scalability and consider performance for large-scale databases. </li>
    <li>Support analytical queries such as the number of articles published in journals in specific years and quarters, articles published per quarter across all years, and number of unique authors publishing articles per year. </li>
</ol>


### Part 3: Explore and Mine Data

<ol>
    <li>Create an R Notebook named <b>AnalyzeData.LastNameF.Rmd</b> within your R Project for Part 3.</li>
    <li>Write a report in markdown format, presenting the results of the following analytical queries against your MySQL data warehouse from Part 2:</li>
    <li>Analytical Query I: Top five journals with the most articles published within a specific time period.</li>
    <li>Analytical Query II: Number of articles per journal per year, broken down by quarter. Choose an appropriate way to present the information, such as using tables or visualizations.</li>
    <li>Use data from the fact tables to generate the report.</li>
</ol>