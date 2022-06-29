from server import hechos
from flask import Flask,jsonify,request

app=Flask(__name__)

#app.secret_key="apiRest"

@app.route('/etl/hechos/<int:date>/<int:category>',methods=['GET'])
def consultarcategoria(h_date,category):
    respuesta=hechos.category(h_date,category)
    return respuesta

@app.route('/etl',methods=['GET'])
def saludo():
    print('hola mundo')