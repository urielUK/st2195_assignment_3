import sqlite3
import pandas as pd
import os

db_path=r"C:/Users/urisc/OneDrive/Documents/Maths and Econ/ST2195/Data Set/airline2.db"
if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    print("Connection successful!")
else:
    print("error: bad file meh")

# Connect to the same database
#conn = sqlite3.connect('../airline2.db')


# Example Query: Q1
query = """
SELECT UniqueCarrier, COUNT(*) as flight_count 
FROM ontime 
GROUP BY UniqueCarrier 
ORDER BY flight_count DESC 
LIMIT 5
"""

df = pd.read_sql_query(query, conn)
df.to_csv('q1_python.csv', index=False)

conn.close()
