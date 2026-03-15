library(DBI)
library(RSQLite)
library(readr)

# Connect to existing DB
conn <- dbConnect(RSQLite::SQLite(), "../airline2.db")

# Simplified Query: One block to join, filter, count, and sort
q3_simple_sql <- "
  SELECT c.Description AS carrier_name, COUNT(*) AS total_cancelled
  FROM ontime o
  JOIN carriers c ON o.UniqueCarrier = c.Code
  WHERE o.Cancelled = 1 
  GROUP BY carrier_name
  ORDER BY total_cancelled DESC
  LIMIT 1
"

# Execute and save in one step
write_csv(dbGetQuery(conn, q3_simple_sql), "q3_simplified.csv")

dbDisconnect(conn)
