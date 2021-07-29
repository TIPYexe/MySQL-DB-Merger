import mysql.connector
import re

# 1 = branch to merge
# 2 = main

# ex: provider, providerregions
#   : regions, providerregions

def mergeTabels(db_to_merge, db_main, table1, table2, key_1, key_2):
    mydb1 = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database=db_to_merge
    )

    mydb2 = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database=db_main
    )

    sameIdConflictIssueSolver(mydb1, mydb2, table1, table2, key_1, key_2)
    mergeTheNewData(mydb1, mydb2, table1, table2)
    mydb2.commit()

# will assign a new and unique ID to each
def sameIdConflictIssueSolver(mydb1, mydb2, table1, table2, key_1, key_2):
    mycursor1 = mydb1.cursor()
    mycursor2 = mydb2.cursor()

    mycursor1.execute("SELECT * FROM " + table1)
    myresult1 = mycursor1.fetchall()

    mycursor2.execute("SELECT * FROM " + table2)
    myresult2 = mycursor2.fetchall()

    # newId = last (hopefully) highest Id from the main DB + 1
    newId = myresult2[len(myresult2) - 1][0] + 1

    # TODO: optimize seaerch
    for index1 in myresult1:
        for index2 in myresult2:
            # if they have the same ID
            if index1[0] == index2[0]:
                # but they have different values
                if index1[1] != index2[1]:
                    # I update manually the Id in all the tables where it appeared
                    # TODO: see if I can extract the connections between tables in order to make the changes reccursive
                    oldId = str(index1[0])
                    mycursor1.execute\
                        ("UPDATE " + table1 + " SET `"+ key_1 +"` = '" + newId + "' WHERE (`id` = '" + oldId + "');")
                    mycursor1.execute\
                        ("UPDATE " + table2 + " SET `"+ key_2 +"` = '" + newId + "' WHERE (`id` = '" + oldId + "');")

                    newId += 1

    mydb1.commit()

def mergeTheNewData(mydb1, mydb2, table1, table2):
    mycursor1 = mydb1.cursor()
    mycursor2 = mydb2.cursor()

    mycursor1.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'" + table1 + "' ORDER BY ORDINAL_POSITION ASC")
    columnsRaw = mycursor1.fetchall()
    columnNames = list(map(lambda x: re.sub("[(),']", "", str(x)), columnsRaw))
    columnCommand = "`" + "`, `".join(columnNames) + "`"

    print(columnsRaw)
    print(columnCommand)

    mycursor1.execute("SELECT * FROM " + table1)
    myresult1 = mycursor1.fetchall()

    mycursor2.execute("SELECT * FROM " + table2)
    myresult2 = mycursor2.fetchall()

    for index1 in myresult1:
        for index2 in myresult2:
            found = False
            # if a new ID have been found
            if index1[0] == index2[0]:
                found = True
                break

            if found == False:
                # daca mergeuim bd1 in 2. Atunci join(index1) + mycursor2
                # daca merge-uim 2 in 1. Atunci join(index2) + mycursor1
                dataRaw = list(map(lambda x: str(x), index1))
                dataCommand = "`" + "`, `".join(dataRaw) + "`"
                mycursor2.execute("INSERT INTO "+ table1 +" ("+ columnCommand +") VALUES ("+ dataCommand +");")

    mydb2.commit()


mergeTabels("urbanair-1", "urbanair-2", "providers", "providerregions", "id", "providerId")