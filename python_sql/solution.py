import sqlite3
import pandas as pd
import os

# Part 1 - Initial setup
# Connect to the database (creates it if it doesn't exist)
conn = sqlite3.connect("airline2.db")

# Part 2 - Creating the database
# Load and write lookup tables
pd.read_csv("airports.csv").to_sql("airports", conn, if_exists="replace", index=False)
pd.read_csv("carriers.csv").to_sql("carriers", conn, if_exists="replace", index=False)
pd.read_csv("plane-data.csv").to_sql("planes", conn, if_exists="replace", index=False)

years = range(2000, 2006)

for year in years:
    filename = f"{year}.csv.bz2"
    print(f"Processing: {filename}")
    
    # Read the compressed CSV
    # low_memory=False helps with mixed data types in large datasets
    tmp_data = pd.read_csv(filename, compression='bz2', low_memory=False)
    
    # Append to the 'ontime' table
    tmp_data.to_sql("ontime", conn, if_exists="append", index=False)
    
    # Clean up memory
    del tmp_data

# Create indexes for better performance
cursor = conn.cursor()
cursor.execute("CREATE INDEX year_idx ON ontime(Year)")
cursor.execute("CREATE INDEX dest_idx ON ontime(Dest)")
cursor.execute("CREATE INDEX origin_idx ON ontime(Origin)")
conn.commit()

# Part 3 - Queries

# Verification count
total_rows = pd.read_sql_query("SELECT count(*) FROM ontime", conn)
print(f"Total rows in database: {total_rows.iloc[0,0]}")

# --- Question 1: Lowest Average Departure Delay ---
q4_sql = """
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
"""
q4_res = pd.read_sql_query(q4_sql, conn)
q4_res.to_csv("q4_python.csv", index=False)

# --- Question 2: Highest Inbound Flights ---
q_city_sql = """
  SELECT a.city, COUNT(*) AS flight_count
  FROM ontime o
  JOIN airports a ON o.Dest = a.iata
  WHERE o.Cancelled = 0 
    AND a.city IN ('Chicago', 'Atlanta', 'New York', 'Houston')
  GROUP BY a.city
  ORDER BY flight_count DESC
  LIMIT 1
"""
q_city_res = pd.read_sql_query(q_city_sql, conn)
q_city_res.to_csv("q_city_python.csv", index=False)

# --- Question 3: Highest Number of Cancelled Flights ---
q_carrier_sql = """
  SELECT c.Description AS carrier_name, COUNT(*) AS total_cancelled
  FROM ontime o
  JOIN carriers c ON o.UniqueCarrier = c.Code
  WHERE o.Cancelled = 1 
    AND c.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 
                          'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
  GROUP BY c.Description
  ORDER BY total_cancelled DESC
  LIMIT 1
"""
q_carrier_res = pd.read_sql_query(q_carrier_sql, conn)
q_carrier_res.to_csv("q_carrier_cancelled_python.csv", index=False)

# --- Question 4: Highest Cancellation Ratio ---
q_ratio_sql = """
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
"""
q_ratio_res = pd.read_sql_query(q_ratio_sql, conn)
q_ratio_res.to_csv("q_ratio_python.csv", index=False)

# Close connection
conn.close()
print("All Python queries completed and CSVs saved.")
