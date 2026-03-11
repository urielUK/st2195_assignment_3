#install.packages(c("DBI", "RSQLite", "readr"))
library(DBI)
library(RSQLite)
library(readr)
# Create/Connect to the database
conn <- dbConnect(RSQLite::SQLite(), "airline2.db")

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

# This asks the database to count every single row in the 'ontime' table
dbGetQuery(conn, "SELECT count(*) FROM ontime")

# Disconnect when finished
dbDisconnect(conn)

