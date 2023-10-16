import oracledb
import os
import pandas as pd
import xlsxwriter
from datetime import datetime, timedelta
class DatabaseUtil:
    def __init__(self) -> None:
        self.host = "localhost"
        self.port = 1521
        self.service_name = "XEPDB1"
        self.user = "system"
        self.password = "admin"
        self.connection = self.establish_connection()

    def establish_connection(self):
        try:
            connection = oracledb.connect(
                user=self.user,
                password=self.password,
                dsn=f"{self.host}:{self.port}/{self.service_name}"
            )
            print("Successfully connected to the database")
            return connection
        except oracledb.DatabaseError as e:
            print("Error connecting to Oracle:", e)
            return 
        
    def execute_select_query(self, query):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            data = cursor.fetchall()
            if data:
                col_names = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(data, columns=col_names)
                print(df)
                return df
        except oracledb.DatabaseError as e:
            print("Error executing SELECT query:", e)
            return None
        except oracledb.DatabaseError as e:
            print("Error executing SELECT query:", e)
            return None


    def close_connection(self):
        if self.connection:
            self.connection.close()
            print("Database connection closed")
            
            
    def save_dataframe_into_excel(self, df, excel_path):
        try:
            excel_writer = pd.ExcelWriter(excel_path, engine='xlsxwriter')
            df.to_excel(excel_writer, sheet_name='Sheet1', index=False)

            # Get the xlsxwriter workbook and worksheet objects.
            workbook = excel_writer.book
            worksheet = excel_writer.sheets['Sheet1']

            # Define a date format for the cells
            date_format = workbook.add_format({'num_format': 'yyyy-mm-dd'})

            # Apply the date format to date columns
            for i, col in enumerate(df.columns):
                if pd.api.types.is_datetime64_ns_dtype(df[col]):
                    worksheet.set_column(i, i, 12, date_format)

            excel_writer.save()
            print(f"Data written to {excel_path}")
        except Exception as e:
            print(f"Error saving DataFrame to Excel : {e}")        
    def read_sql_query(self, index):
        file_path = os.path.join(os.getcwd(),"data", "aafalu_quiries.sql")
        print(file_path)
        with open(file_path, "r") as file:
            sql_commands= file.read().split(';')
            return sql_commands[index].strip()

# Create an instance of the DatabaseUtil class
my_class = DatabaseUtil()
my_class.establish_connection()
query = my_class.read_sql_query(0)
print(f"query from .sql file : {query}")
df= my_class.execute_select_query(query)
my_class.save_dataframe_into_excel(df, "result1.xlsx")

# To close the connection when you are done:
# my_class.close_connection()
