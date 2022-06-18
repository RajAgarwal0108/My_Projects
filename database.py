import mysql.connector as connection
import logging as lg
import csv

import pandas as pd
import pymongo

lg.basicConfig(filename='file.log', level=lg.INFO)


# creating a sql database class
class sql_database:

    def __init__(self,host,user,passwd,db):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.auth_plugin = "mysql_native_password"

    # building connection with mysql_workbench
    def build_mysql_connection(self):
        """
        Docstring:
        build_mysql_connection(self)

        Build the connection between mysql and python using mysql-connector
        :return:
        """
        try:
            return connection.connect(host=self.host,user = self.user,passwd = self.passwd,auth_plugin = self.auth_plugin,use_pure=True)
        except Exception as e:
            print("error-occured:",str(e))
            lg.error(str(e))
            return str(e)
    # creating a new database if given doesn't exists
    def handle_database(self):
        """
        Docstring:
        handle_database(self)

        checks whether selected database exists or not, if not then create it

        :return:
        """
        try:
            mydb = self.build_mysql_connection()
            if mydb.is_connected():
                cursor = mydb.cursor()
                cursor.execute("SHOW DATABASES LIKE '{}' ".format(self.db))
                if not cursor.fetchall():
                    cursor.execute("CREATE DATABASE {}".format(self.db))
                    lg.info("created new database {}".format(self.db))
                return True
        except Exception as e:
            print("error-occured:", str(e))
            lg.error(str(e))
            return str(e)



    # creating a table
    def create_table(self, table_details):
        """
        Docstring:
        create_table(table_details)

        create a new table inside the database
        Format:
        table_details : {'<table_name>':
                            {'<field1>' : '<field_type>',
                             '<field2>' : '<field_type>'},
                             ....}
        :return:
        """
        try:
            mydb = self.build_mysql_connection()
            if mydb.is_connected():
                cursor = mydb.cursor()
                if self.handle_database():
                    print(table_details)
                    def create(): #create() function to create a new table
                        cursor.execute('USE {}'.format(self.db))
                        table_name = list(table_details.keys())[0]
                        schema = table_details['{}'.format(table_name)]
                        s = ' '
                        for i in schema:
                            s = s + str(i + " " + schema[i] + ", ")
                        query = "CREATE TABLE {ta}({sch})".format(ta=table_name, sch=s[:-2])
                        cursor.execute(query)
                        return "table created sucessfully..."
                    create()
                    lg.info("new table created sucessfully{}".format(list(table_details.keys())[0]))
                    return "table created sucessfully..."
        except Exception as e:
            print("error-occured:", str(e))
            lg.error(str(e))
            return str(e)

    def insert_one(self,table_name,insert_info):
        """
        Docstring:
        insert_one(table_name,insert_info)

        :param table_name: name of the table
        :param insert_info: field name and value

        Format:
        table_name : <table_name>
        insert_info : {"<field1>" : "<value>",
                       "<field2>" : "<value>",
                        ....}
        :return:
        """

        try:
            mydb = self.build_mysql_connection()
            if mydb.is_connected():
                cursor = mydb.cursor()
                if self.handle_database():
                    try:
                        cursor.execute('use {}'.format(self.db))
                        cursor.execute("SHOW TABLES LIKE '{}' ".format(table_name))
                        if cursor.fetchall():
                            col = insert_info.keys()
                            cols = str(list(col))
                            val = insert_info.values()
                            vals = str(list(val))
                            s = ' '
                            for i in cols[1:-1]:
                                if i == "'":
                                    continue
                                s = s + i
                            query = "INSERT INTO {table} ({column}) values({info})".format(table = table_name,column = s,info =vals[1:-1])
                            cursor.execute(query)
                            cursor.execute("select * from {}".format(table_name))
                            result = cursor.fetchall()
                            mydb.commit()
                            lg.info(f"record inserted,{result[-1]} ")
                            return result[-1]
                    except Exception as e:
                        print('error-occured:',str(e))
                        lg.error(str(e))
                        return str(e)
        except Exception as e:
            print('error-occured:', str(e))
            lg.error(str(e))
            return str(e)




    def insert_file(self,table_name,file_name):

        """
        Docstring:
        insert_file(table_name,file_name)
        Insert all the data from the file into the sql table

        :param table_name: name of the table
        :param file_name: path of the file which have to be read
        :return:
        """
        try:
            mydb = self.build_mysql_connection()
            if mydb.is_connected():
                cursor = mydb.cursor()
                if self.handle_database():
                    try:
                        cursor.execute('use {}'.format(self.db))
                        with open(file_name,'r') as data:
                            next(data)
                            data_csv = csv.reader(data,delimiter = "\n")
                            for i,j in enumerate(data_csv):
                                query = "INSERT INTO {t} values ({d})".format(t = table_name,d = str(j)[2:-2])
                                cursor.execute(query)
                            mydb.commit()
                            lg.info("insertion successful")
                            return "insertion successful"
                    except Exception as e:
                        print("error-occured",str(e))
                        lg.error(str(e))
                        return (str(e))
        except Exception as e:
            print("error-occured", str(e))
            lg.error(str(e))
            return (str(e))


    def update(self,table_name,query):

        """
        Docstring:
        update(table_name,query)

        Make changes inside the table according to the condition

        :param table_name: name of the table
        :param query: sql_query to make changes in the table

        query : "set (<field_name> = <value> , ...) where (<field_name> = <condition>,....)
        :return:
        """
        try:
            mydb = self.build_mysql_connection()
            if mydb.is_connected():
                cursor = mydb.cursor()
                if self.handle_database():
                    try:
                        cursor.execute('USE {}'.format(self.db))
                        update_query = f"update {table_name} {query}"
                        cursor.execute(update_query)
                        mydb.commit()
                        lg.info(f"{table_name} updated successfully by query {update_query}")
                        return f"{table_name} updated successfully by query {update_query}"
                    except Exception as e:
                        print('error-occurred', str(e))
                        lg.error(str(e))
                        return str(e)
        except Exception as e:
            print('error-occurred',str(e))
            lg.error(str(e))
            return str(e)




    def remove(self,table_name,condition):

        """
        Docstring:
        remove(table_name,condition)

        Delete records from the table based on the conditions

        :param table_name: name of table
        :param condition: conditions to be used

        condition = {<field_name> : <value>, ...}
        :return:
        """
        try:
            mydb = self.build_mysql_connection()
            if mydb.is_connected():
                cursor = mydb.cursor()
                if self.handle_database():
                    try:
                        cursor.execute('USE {}'.format(self.db))
                        delete_query = f"delete from {table_name} where {condition}"
                        cursor.execute(delete_query)
                        mydb.commit()
                        lg.info(f"{table_name} updated successfully by query {delete_query}")
                        return f"{table_name} updated successfully by query {delete_query}"
                    except Exception as e:
                        print('error-occurred', str(e))
                        lg.error(str(e))
                        return str(e)
        except Exception as e:
            print('error-occurred',str(e))
            lg.error(str(e))
            return str(e)
    def download(self,table_name,file_name):

        """
        Docstring:
        download(table_name,file_name)

        To store all the records of the data into csv file

        :param table_name: name of the table
        :param file_name: file path where data has to be saved

        Examole:
        file_name = "/home/file/sql.csv"
        :return:
        """
        try:
            mydb = self.build_mysql_connection()
            if mydb.is_connected():
                cursor = mydb.cursor()
                if self.handle_database():
                    try:
                        cursor.execute('USE {}'.format(self.db))
                        query = f"select * from {table_name}"
                        cursor.execute(query)
                        with open(file_name,'w') as data:
                            writer = csv.writer(data)
                            for i in cursor.fetchall():
                                writer.writerow(i)
                        lg.info(f"download successful, file path : {file_name}")
                        return "download successful"
                    except Exception as e:
                        print('error-occurred', str(e))
                        lg.error(str(e))
                        return str(e)
        except Exception as e:
            print('error-occurred', str(e))
            lg.error(str(e))
            return str(e)




class no_sql_database:
    def __init__(self,db):
        self.db = db
        self.host = 'mongodb://localhost:27017'


    def create_table(self,table_name):
        try:
            client = pymongo.MongoClient(self.host)
            mydb = client[self.db]
            collection = mydb[table_name]
            lg.info(f"table created{table_name}")
            return "table created"
        except Exception as e:
            print('error-occurred', str(e))
            lg.error(str(e))
            return str(e)



    def insert_one(self,table_name,record):
        try:
            client = pymongo.MongoClient(self.host)
            mydb = client[self.db]
            collection = mydb[table_name]
            collection.insert_one(record)
            lg.info(f"record inserted {record}")
            return "record inserted"
        except Exception as e:
            print('error-occurred', str(e))
            lg.error(str(e))
            return str(e)




    def insert_file(self,table_name,file_path):
        try:
            client = pymongo.MongoClient(self.host)
            mydb = client[self.db]
            collection = mydb[table_name]
            csv_file = open(file_path,'r')
            print(file_path)
            reader = csv.DictReader(csv_file)
            for each in reader:
                collection.insert_one(each)
            lg.info(f"file inserted successfully {file_path}")
            return "file inserted successfully"
        except Exception as e:
            print('error-occurred', str(e))
            lg.error(str(e))
            return str(e)
    def update(self,table_name,condition,update):
        try:
            client = pymongo.MongoClient(self.host)
            mydb = client[self.db]
            collection = mydb[table_name]
            update_query = {'$set':update}
            collection.update_many(condition,update_query)
            return "record updated successfully"

        except Exception as e:
            print('error-occurred', str(e))
            lg.error(str(e))
            return str(e)
    def remove(self,table_name,condition):
        try:
            client = pymongo.MongoClient(self.host)
            mydb = client[self.db]
            collection = mydb[table_name]
            collection.delete_many(condition)
            lg.info(f"record deleted successfully , condition = {condition}")
            return "record deleted successfully"

        except Exception as e:
            print('error-occurred', str(e))
            lg.error(str(e))
            return str(e)
    def download(self,table_name,file_path):
        try:
            client = pymongo.MongoClient(self.host)
            mydb = client[self.db]
            collection = mydb[table_name]
            cursor = collection.find()
            cursor = pd.DataFrame(list(cursor))
            cursor = cursor[cursor.columns[1:]]
            cursor.to_csv(file_path)
            lg.info(f"data downloaded , file_path : {file_path}")
            return f"data downloaded{file_path}"

        except Exception as e:
            print('error-occurred', str(e))
            lg.error(str(e))
            return str(e)











