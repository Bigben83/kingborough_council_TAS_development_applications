require 'nokogiri'
require 'open-uri'
require 'sqlite3'
require 'logger'
require 'date'

# Initialize the logger
logger = Logger.new(STDOUT)

# Define the URL of the page
url = 'https://www.kingborough.tas.gov.au/development/planning-notices/'

# Step 1: Fetch the page content
begin
  logger.info("Fetching page content from: #{url}")
  page_html = open(url).read
  logger.info("Successfully fetched page content.")
rescue => e
  logger.error("Failed to fetch page content: #{e}")
  exit
end

# Step 2: Parse the page content using Nokogiri
doc = Nokogiri::HTML(page_html)

# Step 3: Initialize the SQLite database
db = SQLite3::Database.new "data.sqlite"

# Create table
db.execute <<-SQL
  CREATE TABLE IF NOT EXISTS kingborough (
    id INTEGER PRIMARY KEY,
    description TEXT,
    date_scraped TEXT,
    date_received TEXT,
    on_notice_to TEXT,
    address TEXT,
    council_reference TEXT,
    applicant TEXT,
    owner TEXT,
    stage_description TEXT,
    stage_status TEXT,
    document_description TEXT,
    title_reference TEXT
  );
SQL

# Define variables for storing extracted data for each entry
address = ''  
description = ''
on_notice_to = ''
title_reference = ''
date_received = ''
council_reference = ''
applicant = ''
owner = ''
stage_description = ''
stage_status = ''
document_description = ''
date_scraped = ''

# Find all table rows in the table with id 'list'
doc.css('#list tbody tr').each do |row|
  council_reference = row.css('td')[0].text.strip
  address = row.css('td')[1].text.strip
  date_received = row.css('td')[2].text.strip
  on_notice_to = row.css('td')[3].text.strip
  description = row.css('td')[4].text.strip
  document_description = row.css('td a').map { |link| link['href'] }.join(", ")

  date_scraped = Date.today.to_s

  # Format the date to ISO 8601
  begin
    date_received = Date.strptime(date_received, '%d %b %Y').strftime('%Y-%m-%d')
  rescue => e
    logger.error("Date format issue: #{e.message}")
  end

  # Step 6: Ensure the entry does not already exist before inserting
  existing_entry = db.execute("SELECT * FROM kingborough WHERE council_reference = ?", council_reference )

  if existing_entry.empty? # Only insert if the entry doesn't already exist
  # Step 5: Insert the data into the database
  db.execute("INSERT INTO kingborough (council_reference, address, description, document_description date_received, on_notice_to, date_scraped) VALUES (?, ?, ?, ?, ?, ?, ?)",
             [council_reference, address, description, document_description, date_received, on_notice_to, date_scraped])

  logger.info("Data for #{council_reference} saved to database.")
    else
      logger.info("Duplicate entry for application #{council_reference} found. Skipping insertion.")
    end
end

# Finish
logger.info("Data has been successfully inserted into the database.")
