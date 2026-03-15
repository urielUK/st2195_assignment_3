#Part 1 - Initial setup

#install.packages(c("DBI", "RSQLite", "readr"))
library(DBI)
library(RSQLite)
library(readr)
# Create/Connect to the database
conn <- dbConnect(RSQLite::SQLite(), "airline2.db")

#Part 2 - Creating the spreadsheets/database

# Load and write airports
airports <- read_csv("airports.csv")
dbWriteTable(conn, "airports", airports, overwrite = TRUE)

# Load and write carriers
carriers <- read_csv("carriers.csv")
dbWriteTable(conn, "carriers", carriers, overwrite = TRUE)

# Load and write planes
planes <- read_csv("plane-data.csv")
dbWriteTable(conn, "planes", planes, overwrite = TRUE)
years <- 2000:2005

for (year in years) {
  filename <- paste0(year, ".csv.bz2")
  cat("Processing:", filename, "\n")
  
  # Read the CSV (read_csv handles .bz2 automatically)
  tmp_data <- read_csv(filename, show_col_types = FALSE)
  
  # Append to the 'ontime' table
  # If it's the first year (2000), it creates the table; otherwise, it appends.
  dbWriteTable(conn, "ontime", tmp_data, append = TRUE)
  
  # Clean up memory
  rm(tmp_data)
  gc()
}

# Create indexes for better performance
dbExecute(conn, "CREATE INDEX year_idx ON ontime(Year)")
dbExecute(conn, "CREATE INDEX dest_idx ON ontime(Dest)")
dbExecute(conn, "CREATE INDEX origin_idx ON ontime(Origin)")

#Part 3 - Queries

# This asks the database to count every single row in the 'ontime' table
dbGetQuery(conn, "SELECT count(*) FROM ontime")

#Question 1

target_models <- c('737-230', 'ERJ 190-100 IGW', 'A330-223', '737-282')

#DBI (SQL Notation)
q4_sql <- "
  SELECT p.model, AVG(o.DepDelay) AS avg_dep_delay
  FROM ontime o
  JOIN planes p ON o.TailNum = p.tailnum
  WHERE o.Cancelled = 0 
    AND o.Diverted = 0 
    AND o.DepDelay IS NOT NULL
    AND p.model IN ('737-230', 'ERJ 190-100 IGW', 'A330-223', '737-282')
  GROUP BY p.model
  ORDER BY avg_dep_delay ASC
  LIMIT 1
"
q4_dbi_res <- dbGetQuery(conn, q4_sql)
write_csv(q4_dbi_res, "q4_dbi.csv")


# dplyr Notation

# This translates R code into SQL
ontime_db <- tbl(conn, "ontime")
planes_db <- tbl(conn, "planes")

q4_dplyr_res <- ontime_db %>%
  inner_join(planes_db, by = c("TailNum" = "tailnum")) %>%
  filter(
    Cancelled == 0, 
    Diverted == 0, 
    !is.na(DepDelay),
    model %in% target_models
  ) %>%
  group_by(model) %>%
  summarise(avg_dep_delay = mean(DepDelay, na.rm = TRUE)) %>%
  arrange(avg_dep_delay) %>%
  head(1) %>%
  collect()

write_csv(q4_dplyr_res, "q4_dplyr.csv")

#Question 2

# Define the target cities
target_cities <- c('Chicago', 'Atlanta', 'New York', 'Houston')

#DBI (SQL Notation)
# We join on Dest = iata and filter by city
q_city_sql <- "
  SELECT a.city, COUNT(*) AS flight_count
  FROM ontime o
  JOIN airports a ON o.Dest = a.iata
  WHERE o.Cancelled = 0 
    AND a.city IN ('Chicago', 'Atlanta', 'New York', 'Houston')
  GROUP BY a.city
  ORDER BY flight_count DESC
  LIMIT 1
"
q_city_dbi_res <- dbGetQuery(conn, q_city_sql)
write_csv(q_city_dbi_res, "q_city_dbi.csv")

#dplyr Notation

airports_db <- tbl(conn, "airports")

q_city_dplyr_res <- ontime_db %>%
  filter(Cancelled == 0) %>%
  inner_join(airports_db, by = c("Dest" = "iata")) %>%
  filter(city %in% target_cities) %>%
  group_by(city) %>%
  summarise(flight_count = n()) %>%
  arrange(desc(flight_count)) %>%
  head(1) %>%
  collect()

write_csv(q_city_dplyr_res, "q_city_dplyr.csv")

#Question 3

# Define the target companies
target_carriers <- c('United Air Lines Inc.', 'American Airlines Inc.', 
                     'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')

#DBI (SQL Notation) ---
# We join on UniqueCarrier = Code and filter by Description
q_carrier_sql <- "
  SELECT c.Description AS carrier_name, COUNT(*) AS total_cancelled
  FROM ontime o
  JOIN carriers c ON o.UniqueCarrier = c.Code
  WHERE o.Cancelled = 1 
    AND c.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 
                          'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
  GROUP BY c.Description
  ORDER BY total_cancelled DESC
  LIMIT 1
"
q_carrier_dbi_res <- dbGetQuery(conn, q_carrier_sql)
write_csv(q_carrier_dbi_res, "q_carrier_cancelled_dbi.csv")

#dplyr Notation ---
carriers_db <- tbl(conn, "carriers")

q_carrier_dplyr_res <- ontime_db %>%
  filter(Cancelled == 1) %>%
  inner_join(carriers_db, by = c("UniqueCarrier" = "Code")) %>%
  filter(Description %in% target_carriers) %>%
  group_by(carrier_name = Description) %>%
  summarise(total_cancelled = n()) %>%
  arrange(desc(total_cancelled)) %>%
  head(1) %>%
  collect()

write_csv(q_carrier_dplyr_res, "q_carrier_cancelled_dplyr.csv")

#Question 4
#Bit unneccesary, already defined in previous query
#Define the list of companies from your multiple-choice options
target_carriers <- c('United Air Lines Inc.', 'American Airlines Inc.', 
                     'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')

#DBI (SQL Notation) ---
# We use AVG(Cancelled) because it's a 0/1 column. 
# The average is mathematically identical to the cancellation ratio.
q_ratio_sql <- "
  SELECT 
    c.Description AS carrier_name, 
    AVG(o.Cancelled) AS cancellation_ratio
  FROM ontime o
  JOIN carriers c ON o.UniqueCarrier = c.Code
  WHERE c.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 
                          'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
  GROUP BY c.Description
  ORDER BY cancellation_ratio DESC
  LIMIT 1
"
q_ratio_dbi_res <- dbGetQuery(conn, q_ratio_sql)
write_csv(q_ratio_dbi_res, "q_ratio_dbi.csv")


#dplyr Notation ---
# Same logic, but using R's pipe (%>%) syntax
carriers_db <- tbl(conn, "carriers")

q_ratio_dplyr_res <- ontime_db %>%
  inner_join(carriers_db, by = c("UniqueCarrier" = "Code")) %>%
  filter(Description %in% target_carriers) %>%
  group_by(carrier_name = Description) %>%
  summarise(
    cancellation_ratio = mean(Cancelled, na.rm = TRUE)
  ) %>%
  arrange(desc(cancellation_ratio)) %>%
  head(1) %>%
  collect()

write_csv(q_ratio_dplyr_res, "q_ratio_dplyr.csv")

# Disconnect when finished
dbDisconnect(conn)





