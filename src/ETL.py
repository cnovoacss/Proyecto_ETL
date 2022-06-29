from dataclasses import replace
from flask import Flask,jsonify
from datetime import datetime
#from flaskext.mysql import MySQL
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector as sql
from datetime import datetime, timedelta
import calendar

servicio=Flask(__name__)

#######################################################################################################
# Conexión de la base de datos
engine = create_engine('mysql+mysqldb://root:123456@localhost:3306/proyecto_python', echo = False)


########################################Extracción#####################################################
# Cargue de archivos csv del data lake
# Archivo de producto
columns_products=['product_id','product_category_id','product_name','product_description','product_price','product_image']
products_csv=pd.read_csv("data/products",sep='|',header=None,names=columns_products)
products_csv.columns = products_csv.columns.str.replace('product_category_id', 'category_id')

# Archivo de cliente
columns_customer=['customer_id','customer_fname','customer_lname','customer_password1','customer_password2','customer_street','customer_state','customer_city','customer_zipcode']
customer_csv=pd.read_csv("data/customer",sep='|',header=None,names=columns_customer)

# Archivo de ordenes
columns_orders=['order_id','order_date','order_customer_id','order_status']
orders_csv=pd.read_csv("data/orders",sep='|',header=None,names=columns_orders)
orders_csv['order_date']=orders_csv['order_date'].str[:10]

# Archivo de detalle de ordenes
columns_order_items=['order_item_id','order_item_order','order_item_product_id','order_item_quantity','order_item_subtotal','order_item_product_price']
order_items_csv=pd.read_csv("data/order_items",sep='|',header=None,names=columns_order_items)

# Archivo de categorias
columns_categories=['category_id','category_department_id','category_name']
categories_csv=pd.read_csv("data/categories",sep='|',header=None,names=columns_categories)

# Archivo de departamentos
columns_departments=['department_id','department_name']
departments_csv=pd.read_csv("data/departments",sep='|',header=None,names=columns_departments)

#############################################Transformación############################################

#Creación de lista con el año mes y día para ser usado en la dimensión de fecha
inicio = datetime(2000,10,1)
fin    = datetime(2022,11,5)

lista=[]
lista_fechas = [inicio + timedelta(days=d) for d in range((fin - inicio).days + 1)]
for x in lista_fechas:
    texto=str(x)[0:4]+str(x)[5:7]+str(x)[8:10]
    lista.append([texto,str(x)[0:4],str(x)[5:7],str(x)[8:10],str(x)[0:10]])

#print(lista)
df_dimdate=pd.DataFrame(lista,columns=['id_dimdate','year','month','day','date'])

#Creación de columnas para la dimensión de fecha
lista_mes=[]
lista_semestre=[]
lista_cuatrimestre=[]
lista_trimestre=[]

# Creación de columnas adicionales para obtener mayor detalle de la fecha
for y in df_dimdate["month"]:
    # Lógica para crear nombre de mes
    lista_mes.append(calendar.month_name[int(y)])
    # Lógica para crear semestre
    if int(y)<=6:
        lista_semestre.append(1)
    else:
        lista_semestre.append(2)
    # Lógica para crear cuatrimestre
    if int(y)<=4:
        lista_cuatrimestre.append(1)
    elif int(y)>4 and int(y)<=8:
        lista_cuatrimestre.append(2)
    elif int(y)>8:
        lista_cuatrimestre.append(3)
        
    # Lógica para crear trimestre
    if int(y)<=3:
        lista_trimestre.append(1)
    elif int(y)>3 and int(y)<=6:
        lista_trimestre.append(2)
    elif int(y)>6 and int(y)<=9:
        lista_trimestre.append(3)
    elif int(y)>9:
        lista_trimestre.append(4)

#Creación de nuevas columnas en la dimensión de fecha
df_dimdate["month_name"]=lista_mes
df_dimdate["semester"]=lista_semestre
df_dimdate["quarter"]=lista_cuatrimestre
df_dimdate["trimester"]=lista_trimestre




# Tratamiento de datos para crear la dimensión de ubicación
df_location=customer_csv[['customer_state','customer_city']].drop_duplicates()
df_location['id_location']=df_location.index+1
df_loc=df_location[['id_location','customer_state','customer_city']]
df_loc['key_location']=df_loc['customer_state']+'-'+df_loc['customer_city']




# Tratamiento de datos para crear la dimensión de estados de ordenes
df_orderstatus1=orders_csv[['order_status']].drop_duplicates()
df_orderstatus1.reset_index(inplace=True, drop=False)
df_orderstatus1['id_orderstatus']=df_orderstatus1.index+1
df_orderstatus=df_orderstatus1[['id_orderstatus','order_status']]

departments_csv.reset_index(inplace=True, drop=True)
departments_csv['department_id']=departments_csv.index+1



# Cruce de tablas para crear la tabla de hecho de productos

orders_csv.columns = orders_csv.columns.str.replace('order_customer_id', 'customer_id')
orders_csv.columns = orders_csv.columns.str.replace('order_date', 'date')
products_csv.columns = products_csv.columns.str.replace('product_category_id', 'category_id')
categories_csv.columns = categories_csv.columns.str.replace('category_department_id', 'department_id')
order_items_csv.columns=order_items_csv.columns.str.replace('order_item_order', 'order_id')
order_items_csv.columns=order_items_csv.columns.str.replace('order_item_product_id', 'product_id')



new_df=customer_csv.merge(orders_csv,how='inner',on='customer_id')

new_df=new_df.merge(order_items_csv,how='inner',on='order_id')
new_df.columns=new_df.columns.str.replace('order_item_quantity', 'quantity')
new_df.columns=new_df.columns.str.replace('order_item_subtotal', 'subtotal')

new_df=new_df.merge(products_csv,how='inner',on='product_id')
new_df.columns=new_df.columns.str.replace('order_item_product_price', 'product_price_order')
new_df=new_df.merge(df_dimdate,how='inner',on='date')
new_df['key_location']=new_df['customer_state']+'-'+new_df['customer_city']
new_df=new_df.merge(df_loc,how='inner',on='key_location')
new_df=new_df.merge(df_orderstatus,how='inner',on='order_status')
new_df=new_df.merge(categories_csv,how='inner',on='category_id')
new_df=new_df.merge(departments_csv,how='inner',on='department_id')



new_df['key_hecho']=str(new_df['product_id'])+''+str(new_df['id_dimdate'])+''+str(new_df['id_orderstatus'])+''+str(new_df['id_location'])




# Creación de campos calculados para el hecho producto
df_hecho_product=new_df.groupby(['product_id','id_dimdate','id_orderstatus','id_location']).agg({'quantity':'sum','subtotal':'sum','product_price_order':'mean'}).reset_index()

# Modificación de nombre de la columna de producto
df_hecho_product.columns=df_hecho_product.columns.str.replace('product_id', 'id_dimproduct')


# Creación de campos calculados para el hecho categorias
df_hecho_category=new_df.groupby(['category_id','id_dimdate','id_orderstatus','id_location']).agg({'quantity':'sum','subtotal':'sum'}).reset_index()

# Creación de campos calculados para el hecho departamentos
df_hecho_department=new_df.groupby(['department_id','id_dimdate','id_orderstatus','id_location']).agg({'quantity':'sum','subtotal':'sum'}).reset_index()


#############################################Carga###################################################

# Llenado de dimensiones
df_dimdate.to_sql(name='dimdate',con=engine, if_exists = 'replace', index = False)
products_csv.to_sql(name='dimproducto',con=engine, if_exists = 'replace', index = False)
df_loc.to_sql(name='dimlocation',con=engine, if_exists = 'replace', index = False)
df_orderstatus.to_sql(name='dimorderstatus',con=engine, if_exists = 'replace', index = False)
categories_csv.to_sql(name='dimcategory',con=engine, if_exists = 'replace', index = False)
departments_csv.to_sql(name='dimdepartment',con=engine,if_exists='replace',index=False)


df_hecho_product.to_sql(name='hecho_producto',con=engine, if_exists = 'replace', index = False)
df_hecho_department.to_sql(name='hecho_department',con=engine, if_exists = 'replace', index = False)
df_hecho_category.to_sql(name='hecho_category',con=engine, if_exists = 'replace', index = False)