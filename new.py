import xlrd
import pymysql
pymysql.install_as_MySQLdb()

book = xlrd.open_workbook("CompletedFile.xlsx")
sheet = book.sheet_by_name("Sheet1")

database = pymysql.connect (host="20.86.168.58", user = "ingram", passwd = "Independent12#", db = "RI")

cursor = database.cursor()

# Create the INSERT INTO sql query

query = """CREATE TABLE RI_TABLE(
    id INT NOT NULL AUTO_INCREMENT,
    CustomerCompanyName VARCHAR(255) NOT NULL,
    SubscriptionId VARCHAR(255) NOT NULL,
    ServiceType VARCHAR(255) NOT NULL,
    ResourceName VARCHAR(255) NOT NULL,
    Region VARCHAR(255) NOT NULL,
    ConsumedQuantity VARCHAR(255) NOT NULL,
    InstanceCount VARCHAR(255) NOT NULL,
    ResellerCompanyName VARCHAR(255) NOT NULL,
    PRIMARY KEY (id))"""

query1 = """INSERT INTO RI_TABLE (CustomerCompanyName, SubscriptionId, ServiceType, ResourceName, Region, ConsumedQuantity, InstanceCount, ResellerCompanyName) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

cursor.execute(query)
database.commit()

# Create a For loop to iterate through each row in the XLS file, starting at row 2 to skip the headers
for r in range(1, sheet.nrows):
  CustomerCompanyName	 = sheet.cell(r,0).value
  SubscriptionId	 = sheet.cell(r,1).value
  ServiceType = sheet.cell(r,2).value
  ResourceName	 = sheet.cell(r,3).value
  Region	 = sheet.cell(r,4).value
  ConsumedQuantity = sheet.cell(r,5).value
  InstanceCount	 = sheet.cell(r,6).value
  ResellerCompanyName	 = sheet.cell(r,7).value
		

  # Assign values from each row
  values = (CustomerCompanyName, SubscriptionId, ServiceType, ResourceName, Region, ConsumedQuantity, InstanceCount, ResellerCompanyName)

  # Execute sql Query
  cursor.execute(query1, values)

# Close the cursor
cursor.close()

# Commit the transaction
database.commit()

# Close the database connection
database.close()

# Print results
print ("")
print ("All Done! Bye, for now.")
print ("")
columns = str(sheet.ncols)
rows = str(sheet.nrows)
print ("I just imported " + columns + " columns and " + rows + " rows to MySQL!")

