import sqlite3
import pandas as pd

# Connect to the database (or create it if it doesn't exist)
conn = sqlite3.connect('employee_database.db')
sql_query = "SELECT * from Employee"
df = pd.read_sql(sql_query, conn)

# Close the connection
conn.close()

# Define the path and filename for the Excel file
excel_file = 'employee_data.xlsx'

# Save the data from the dataframe to an XLSX file
df.to_excel(excel_file, index=False)

# Print the dataframe
print(df)
print(f"Data has been saved to {excel_file}")
 