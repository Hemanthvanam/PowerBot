import pyodbc

# Fabric Lakehouse connection info
server = "q2jdkkazwpdufgw7l5isv2yegu-ulyatwdewx3ufjannhm2hew6yi.datawarehouse.fabric.microsoft.com"
database = "bing_lake_db"

# Connection string for AAD Interactive Auth with ODBC Driver 17
conn_str = (
    f"Driver={{ODBC Driver 17 for SQL Server}};"
    f"Server=tcp:{server},1433;"
    f"Database={database};"
    f"Authentication=ActiveDirectoryInteractive;"
    f"Encrypt=yes;"
    f"TrustServerCertificate=no;"
)

# Connect and run test query
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
cursor.execute("SELECT TOP 1 * FROM tbl_sentiment_analysis")
for row in cursor.fetchall():
    print(row)
