from logging import error
from flask import Flask,jsonify
import pymysql
from datetime import datetime
#from conexion import mysql
from flaskext.mysql import MySQL


servicio=Flask(__name__)

servicio.config["MYSQL_DATABASE_USER"] = "root"
servicio.config["MYSQL_DATABASE_PASSWORD"] = "123456"
servicio.config["MYSQL_DATABASE_DB"] = "proyecto_python"
servicio.config["MYSQL_DATABASE_HOST"] = "localhost"
servicio.config['MYSQL_DATABASE_SOCKET'] = None

mysql = MySQL()

mysql.init_app(servicio)


class hechos:

    def category(h_date,category):
        conn = mysql.connect()
        conexion = conn.cursor(pymysql.cursors.DictCursor)
        conexion.execute(f"select * from hecho_category where category_id ={category} or id_dimdate={h_date}")
        datos=conexion.fetchall()

        if (len(datos)>0):
            return jsonify(datos)

        else:
            return jsonify({"Mensaje":"El cliente no existe"})