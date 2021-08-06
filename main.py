import mysql.connector
import re


# 1 = branch to merge
# 2 = main

# ex: provider, providerregions
#   : regions, providerregions

def mergeTabels(db_to_merge, db_main, table1, table2, table3, key_table_1, key_table_2_1, key_table_3, key_table_2_3):
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

    # mycursor1 = mydb1.cursor()

    # # extract column names and concat them in a string, separated by "', '"
    # mycursor1.execute(
    #     "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'" + table1 + "' AND TABLE_SCHEMA = '" + db_to_merge + "' ORDER BY ORDINAL_POSITION ASC")
    # columnsRaw = mycursor1.fetchall()
    # columnNames = list(map(lambda x: re.sub("[(),']", "", str(x)), columnsRaw))
    # columnCommand = "'" + "', '".join(columnNames) + "'"
    #
    # # extract data types
    # # NUMERIC_PRECISION = null for non-numeric values
    # mycursor1.execute(
    #     "SELECT NUMERIC_PRECISION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'" + table1 + "' AND TABLE_SCHEMA = '" + db_to_merge + "' ORDER BY ORDINAL_POSITION ASC")
    # dataTypeRaw = mycursor1.fetchall()
    # dataTypes = list(map(lambda x: re.sub("[(),']", "", str(x)), dataTypeRaw))

    table_1_merge = "`" + db_to_merge + "`.`" + table1 + "`"
    table_2_merge = "`" + db_to_merge + "`.`" + table2 + "`"
    table_1_main = "`" + db_main + "`.`" + table1 + "`"
    table_2_main = "`" + db_main + "`.`" + table2 + "`"
    table_3_main = "`" + db_main + "`.`" + table3 + "`"
    table_3_merge = "`" + db_to_merge + "`.`" + table3 + "`"

    sameIdConflictIssueSolver(mydb1, mydb2, table_1_merge, table_2_merge, table_1_main, key_table_1, key_table_2_1, 0, 2)
    sameIdConflictIssueSolver(mydb1, mydb2, table_3_merge, table_2_merge, table_3_main, key_table_3, key_table_2_3, 0, 1)
    # 0 = id, 2 = type, 0 = skips no columns | 3 = providerId, 4 = regionId, 1 = skips the 1st column
    mergeTheNewData(mydb1, mydb2, table_1_merge, table_1_main, 0, 2, 0, 0)
    mergeTheNewData(mydb1, mydb2, table_3_merge, table_3_main, 0, 1, 0, 2)
    mergeTheNewData(mydb1, mydb2, table_2_merge, table_2_main, 3, 4, 1, 0)
    mydb2.commit()


# will assign a new and unique ID to each
def sameIdConflictIssueSolver(mydb1, mydb2, table_1_merge, table_2_merge, table_1_main, key_1, key_2, i, j):
    mycursos_1_merge = mydb1.cursor()
    mycursor_1_main = mydb2.cursor()

    mycursos_1_merge.execute("SELECT * FROM " + table_1_merge)
    myresult_1_merge = mycursos_1_merge.fetchall()

    mycursor_1_main.execute("SELECT * FROM " + table_1_main)
    myresult_1_main = mycursor_1_main.fetchall()

    # newId_1 = last (hopefully) highest Id from the main DB + 1
    if len(myresult_1_main) > len(myresult_1_merge):
        newId_1 = myresult_1_main[len(myresult_1_main) - 1][i] + 1
    else:
        newId_1 = myresult_1_merge[len(myresult_1_merge) - 1][i] + 1

    # TODO: optimize seaerch
    for index1 in myresult_1_merge:
        for index2 in myresult_1_main:
            # if they have the same ID
            if index1[i] == index2[i]:
                # but they have different values
                if index1[j] != index2[j]:
                    # I update manually the Id in all the tables where it appeared
                    # TODO: see if I can extract the connections between tables in order to make the changes recursive
                    oldId = str(index1[i])

                    # print("UPDATE " + table_1_merge + " SET `" + key_1 + "` = '" + str(newId_1) + "' WHERE `" + key_1 + "` = " + oldId + ";")

                    mycursos_1_merge.execute("UPDATE " + table_1_merge + " SET `" + key_1 + "` = " + str(
                        newId_1) + " WHERE `" + key_1 + "` = " + oldId + ";")
                    mycursos_1_merge.execute("UPDATE " + table_2_merge + " SET `" + key_2 + "` = " + str(
                        newId_1) + " WHERE `" + key_2 + "` = " + oldId + ";")

                    newId_1 += 1

    mydb1.commit()


# TODO: update in providerregions si providers: set metadata = {} where metadata = null

def mergeTheNewData(mydb1, mydb2, table_1_merge, table_1_main, i, j, skip, stop):
    mycursor1 = mydb1.cursor()
    mycursor2 = mydb2.cursor()

    mycursor1.execute("SELECT * FROM " + table_1_merge)
    myresult1 = mycursor1.fetchall()

    mycursor2.execute("SELECT * FROM " + table_1_main)
    myresult2 = mycursor2.fetchall()

    # extract column names and concat them in a string, separated by "', '"
    mycursor1.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'" +
        table_1_main.replace('`', '').split('.')[1] + "' AND TABLE_SCHEMA = '" +
        table_1_main.replace('`', '').split('.')[0] + "' ORDER BY ORDINAL_POSITION ASC")
    columnsRaw = mycursor1.fetchall()
    columnNames = list(map(lambda x: re.sub("[(),']", "", str(x)), columnsRaw))
    columnCommand = ", ".join(columnNames[skip:(len(columnNames)-stop)])

    for index1 in myresult1:
        found = False
        for index2 in myresult2:
            # if a new element have been found, checked by 2 values
            # print(index1[i], index2[i], index1[j], index2[j])
            if index1[i] == index2[i] and index1[j] == index2[j]:
                found = True
                break

        if not found:
            # daca merge-uim bd1 in 2. Atunci join(index1) + mycursor2
            # daca merge-uim 2 in 1. Atunci join(index2) + mycursor1
            dataRaw = list(map(lambda x: str(x), index1))
            dataCommand = "'" + "', '".join(dataRaw[skip:(len(dataRaw)-stop)]) + "'"
            dataCommand = dataCommand.replace("'None'", "''")

            # insert si in tabel_2
            print("INSERT INTO " + table_1_main + " (" + columnCommand + ") VALUES (" + dataCommand + ");")
            mycursor2.execute("INSERT INTO " + table_1_main + " (" + columnCommand + ") VALUES (" + dataCommand + ");")

    mydb2.commit()


# 1st = branch to merge (dev, baza de date la care ne conectam noi, pe server)
# 2nd = main (production de pe slack)
# table2 = tabel de legatura
# [1st-db-name, 2nd-db-name, table1, table2, table3, key_table1, key_to_match_key_table1, key_table3, key_to_match_key_table3]
mergeTabels("urbanair-side", "urbanair-main", "providers", "providerregions", "region", "id", "providerId", "id", "regionId")
