To extract only the data from tables providers, providerregions and region. W/o any create functions
mysqldump -u root --no-create-info urbanair-integrations providers providerregions region > file1.sql

mysqldump -u [user] -p[pass] --no-create-info [db] [tabel1 table2 table3] > [exportFileName].sql
raw 25 = table1 [providers]
raw 35 = table2 [providerregions]
raw 45 = table3 [region]

First try:
mydb = mysql.connector.connect(
  host="localhost",
  user="myusername",
  password="mypassword",
  database="mydatabase"
)
mycursor = mydb.cursor()
mycursor.execute("SELECT * FROM [tabel]")
myresult = mycursor.fetchall()
