from flask import Flask,request,jsonify
from database import sql_database, no_sql_database
import logging as lg



lg.basicConfig(filename='file.log', level = lg.INFO)

app = Flask(__name__)

try :
    @app.route('/sql/create_table',methods = ['POST'])
    def create_table():
        if request.method == 'POST':
            try:
                host = request.json['host']
                user = request.json['user']
                passwd = request.json['passwd']
                db = request.json['db']
                table_details = request.json['table']
                upload = sql_database(host, user, passwd, db)
                return jsonify(upload.create_table(table_details))
            except Exception as e:
                print('error-occurred',str(e))
                lg.error(str(e))
                return str(e)


    @app.route('/sql/insert_one',methods = ['POST'])
    def insert_one():
        if request.method == 'POST':
            try:
                host = request.json['host']
                user = request.json['user']
                passwd = request.json['passwd']
                db = request.json['db']
                table_name = request.json['table']
                insert = request.json['insert']
                upload = sql_database(host, user, passwd, db)
                return jsonify(upload.insert_one(table_name,insert))
            except Exception as e:
                print('error-occurred',str(e))
                lg.error(str(e))
                return str(e)

    @app.route('/sql/insert_file',methods = ['POST'])
    def insert_file():
        if request.method == 'POST':
            try:
                host = request.json['host']
                user = request.json['user']
                passwd = request.json['passwd']
                db = request.json['db']
                table_name = request.json['table']
                file_name = request.json['file_name']
                upload = sql_database(host, user, passwd, db)
                return jsonify(upload.insert_file(table_name,file_name))
            except Exception as e:
                print('error-occurred', str(e))
                lg.error(str(e))
                return str(e)

    @app.route('/sql/update',methods = ['POST'])
    def update():
        if request.method == 'POST':
            try:
                host = request.json['host']
                user = request.json['user']
                passwd = request.json['passwd']
                db = request.json['db']
                table_name = request.json['table']
                query = request.json['query']
                upload = sql_database(host, user, passwd, db)
                return jsonify(upload.update(table_name,query))
            except Exception as e:
                print('error-occurred',str(e))
                lg.error(str(e))
                return str(e)
    @app.route('/sql/remove', methods = ['POST'])
    def remove():
        if request.method == 'POST':
            try:
                host = request.json['host']
                user = request.json['user']
                passwd = request.json['passwd']
                db = request.json['db']
                table_name = request.json['table']
                condition = request.json['condition']
                upload = sql_database(host, user, passwd, db)
                return jsonify(upload.remove(table_name,condition))
            except Exception as e:
                print('error-occurred',str(e))
                lg.error(str(e))
                return str(e)

    @app.route('/sql/download', methods = ['POST'])
    def download():
        if request.method == 'POST':
            try:
                host = request.json['host']
                user = request.json['user']
                passwd = request.json['passwd']
                db = request.json['db']
                table_name = request.json['table']
                file = request.json['file_name']
                upload = sql_database(host, user, passwd, db)
                return jsonify(upload.download(table_name,file))
            except Exception as e:
                print('error-occurred',str(e))
                lg.error(str(e))
                return str(e)




    @app.route('/nosql/create_table',methods = ['POST'])
    def createTable():
        if request.method == 'POST':
            try:
                db = request.json['db']
                table_name = request.json['table']
                upload = no_sql_database(db)
                return jsonify(upload.create_table(table_name))
            except Exception as e:
                print('error-occurred',str(e))
                lg.error(str(e))
                return str(e)

    @app.route('/nosql/insert_one',methods = ['POST'])
    def insertOne():
        if request.method == 'POST':
            try:
                db = request.json['db']
                table_name = request.json['table']
                record = request.json['record']
                upload = no_sql_database(db)
                return jsonify(upload.insert_one(table_name,record))
            except Exception as e:
                print('error-occurred',str(e))
                lg.error(str(e))
                return str(e)


    @app.route('/nosql/insert_file',methods = ['POST'])
    def insertFile():
        if request.method == 'POST':
            try:
                db = request.json['db']
                table_name = request.json['table']
                file_path = request.json['file_path']
                upload = no_sql_database(db)
                return jsonify(upload.insert_file(table_name,file_path))
            except Exception as e:
                print('error-occurred',str(e))
                lg.error(str(e))
                return str(e)

    @app.route('/nosql/update',methods = ['POST'])
    def update_record():
        if request.method == 'POST':
            try:
                db = request.json['db']
                table_name = request.json['table']
                update = request.json['update']
                condition = request.json['condition']
                upload = no_sql_database(db)
                return jsonify(upload.update(table_name,condition,update))
            except Exception as e:
                print('error-occurred',str(e))
                lg.error(str(e))
                return str(e)
    @app.route('/nosql/delete',methods = ['POST'])
    def delete_record():
        if request.method == 'POST':
            try:
                db = request.json['db']
                table_name = request.json['table']
                condition = request.json['condition']
                upload = no_sql_database(db)
                return jsonify(upload.remove(table_name,condition))
            except Exception as e:
                print('error-occurred',str(e))
                lg.error(str(e))
                return str(e)


    @app.route('/nosql/download', methods=['POST'])
    def download_data():
        if request.method == 'POST':
            try:
                db = request.json['db']
                table_name = request.json['table']
                file_path = request.json['file_path']
                upload = no_sql_database(db)
                return jsonify(upload.download(table_name,file_path))
            except Exception as e:
                print('error-occurred',str(e))
                lg.error(str(e))
                return str(e)



    if __name__ == '__main__':
        app.run()

except Exception as e:
    print('error-occurred', str(e))
    lg.error(str(e))
