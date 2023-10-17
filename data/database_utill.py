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
        
    def execute_select_query(self, query, params=None):
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
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
            
            
    def save_dataframe_into_excel(self, df, excel_path, sheet_name):
        try:
            excel_writer = pd.ExcelWriter(excel_path, engine='xlsxwriter')
            df.to_excel(excel_writer, sheet_name=sheet_name, index=False)

            # Get the xlsxwriter workbook and worksheet objects.
            workbook = excel_writer.book
            worksheet = excel_writer.sheets[sheet_name]

            # Define a date format for the cells
            date_format = workbook.add_format({'num_format': 'yyyy-mm-dd'})

            # Apply the date format to date columns
            for i, col in enumerate(df.columns):
                if pd.api.types.is_datetime64_ns_dtype(df[col]):
                    worksheet.set_column(i, i, 12, date_format)

            excel_writer.close()
        
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
query_1=my_class.read_sql_query(0)
query_2 = my_class.read_sql_query(1)
first_name = "Basanta"
df_2= my_class.execute_select_query(query_2, {'first_name':first_name})
df_1= my_class.execute_select_query(query_1)
my_class.save_dataframe_into_excel(df_2, "result2.xlsx","info")
my_class.save_dataframe_into_excel(df_1, "result2.xlsx","all_data")
# To close the connection when you are done:
# my_class.close_connection()
