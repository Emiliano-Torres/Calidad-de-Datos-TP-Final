"""
@author: Emiliano Torres, Franco Vanotti, Juan Anchapuri
"""

#%% Importamos las librerias
import pandas as pd
import matplotlib.pyplot as plt
from inline_sql import sql,sql_val
from json import loads
#%% Importamos las bases de datos
#Emi ""C:\\Users\\emili\\OneDrive\\Desktop\\datos_tp_final_cdl\\""
#comenten la carpeta donde tienen los datos si gustan :D
carpeta=("C:\\Users\\emili\\OneDrive\\Desktop\\datos_tp_final_cdl\\")
maestro_pozos=pd.read_csv(carpeta+"capitulo-iv-pozos.csv")
pozos_no_convencional=pd.read_csv(carpeta+"produccin-de-pozos-de-gas-y-petrleo-no-convencional.csv")
produccion_2024=pd.read_csv(carpeta+"produccin-de-pozos-de-gas-y-petrleo-2024.csv")

#%%Exploracion de los datos
unicidad_maestro_pozos_id=sql^"""SELECT COUNT(DISTINCT idpozo) from maestro_pozos"""
#id es unico para cada registro, buen signo
#%%
unicidad_maestro_pozos_geojson=sql^"""SELECT DISTINCT geojson FROM maestro_pozos"""
#Upaaa pero se repiten geojson solo hay 77804
#%%
unicidad_maestro_pozos_geojson_check=sql^"""SELECT DISTINCT geojson, provincia,COUNT(DISTINCT idpozo) AS repeticiones from maestro_pozos
                                      GROUP BY geojson, provincia HAVING repeticiones>1 ORDER BY repeticiones desc """
#%% Sigla supuesto identificador unico, pero asociado a 11 pozos distintos
unicidad_maestro_pozos_sigla_check=sql^"""SELECT DISTINCT sigla, COUNT(DISTINCT idpozo) AS repeticiones from maestro_pozos
                                      GROUP BY sigla HAVING repeticiones>1 ORDER BY repeticiones desc """
#%%
#Hay ubicaciones con 24 pozos, podriamos tratar de aislarlo
#filas donde hay repeteticiones de coordenadas
filtro_geojson=maestro_pozos[maestro_pozos["geojson"]=='{"type":"Point","coordinates":[-68.311398999999994,-52.549014999999997]}']
#curioso 12 de los 24 son repeticiones, pero en otra region      
#%%      
filtro_siglas=maestro_pozos[maestro_pozos["sigla"]=='APCO.Ch.CR.x-7']

#%% veamos la segunda
unicidad_no_convencional_idpozo=sql^"""SELECT DISTINCT anio,mes,idpozo FROM pozos_no_convencional"""


#%%veamos la tercera tabla
unicidad_produccion_2024_idpozo=sql^"""SELECT DISTINCT mes,idpozo FROM produccion_2024"""

#%%
df_provincias=sql^"""SELECT DISTINCT provincia FROM maestro_pozos"""
Estado_nacional=sql^"""SELECT DISTINCT * FROM maestro_pozos WHERE provincia='Estado Nacional'"""
#%%
df_provincias=df_provincias.drop(7,axis=0)
#%%
df_provincias.to_excel('df_provincias.xlsx')

#%%Separemos las coordenas del string para accederlas mas facil

def separador_de_coordendas(dataframe):
    import json
    dataframe["geojson"]=dataframe["geojson"].apply(json.loads)
    latitud=[]
    longitud=[]
    for index, _ in dataframe.iterrows():
        json=dataframe["geojson"].loc[index]
        latitud.append(json["coordinates"][1])
        longitud.append(json["coordinates"][0])
    
    coordenadas=pd.DataFrame({"latitud":latitud,"longitud":longitud})

    #dataframe.drop("geojson",axis=1,inplace=True)

    dataframe=pd.concat([dataframe.reset_index(drop=True),coordenadas.reset_index(drop=True)],axis=1)
    return dataframe
#%%separamos
maestro_pozos=separador_de_coordendas(maestro_pozos)
#%% Ojo que tarda un rato, lo hice porque lo necesitaba, medio al pedo para ustedes
maestro_pozos.to_excel('maestro_pozos.xlsx')

#%% veamos que pasa con la primera coordenadas repetida
filtro_geojson=maestro_pozos[maestro_pozos["geojson"]=='{"type":"Point","coordinates":[-68.311398999999994,-52.549014999999997]}']
filtro_geojson=separador_de_coordendas(filtro_geojson)
filtro_geojson.to_excel('filtro_geojson.xlsx')
#%%
unicidad_maestro_pozos_geojson_check=sql^"""SELECT DISTINCT geojson, provincia,COUNT(DISTINCT idpozo) AS repeticiones from maestro_pozos
                                      GROUP BY geojson, provincia HAVING repeticiones>1 ORDER BY repeticiones desc """

unicidad_maestro_pozos_geojson_check=separador_de_coordendas(unicidad_maestro_pozos_geojson_check)
unicidad_maestro_pozos_geojson_check=pd.concat([unicidad_maestro_pozos_geojson_check.reset_index(drop=True),df_provincias.reset_index(drop=True)],axis=1)

unicidad_maestro_pozos_geojson_check.to_excel('unicidad_maestro_pozos_geojson_check.xlsx')

#%% cantidad de provincias distintas por geojson
prov_geojson=sql^"""SELECT DISTINCT geojson, COUNT(DISTINCT provincia) AS cantidad_prov FROM maestro_pozos 
                    GROUP BY geojson HAVING cantidad_prov >1 ORDER BY cantidad_prov desc """
#prov_geojson=separador_de_coordendas(prov_geojson)
#prov_geojson.to_excel('prov_geojson.xlsx')

#okey hay solo 8 geojson en 2 provincias distintas.
provincias_conflictivass=sql^"""SELECT DISTINCT m.provincia FROM prov_geojson AS p
                                INNER JOIN maestro_pozos AS m on p.geojson=m.geojson """
                                
#%% mapa
import folium

# Ejemplo de DataFrame con latitudes y longitudes
df=separador_de_coordendas(prov_geojson)
# Crear un mapa centrado en la primera coordenada
mapa = folium.Map(location=[df['latitud'].mean(), df['longitud'].mean()], zoom_start=12)

# Agregar marcadores al mapa
for _, row in df.iterrows():
    folium.Marker(
        location=[row['latitud'], row['longitud']],
        icon=folium.Icon(color='blue', icon='info-sign')  # Personalizar íconos
    ).add_to(mapa)

# Guardar el mapa en un archivo HTML (opcional)
mapa.save('mapa.html')

#%%
cantidad_cuencas=sql^"""SELECT COUNT(DISTINCT cuenca) FROM maestro_pozos"""
pozo_1036=sql^"""SELECT * FROM maestro_pozos WHERE idpozo=212"""

#%% Solucion para unicidad en la primera tabla es borrar los registros duplicados
#en tierra del fuego y mendoza
duplicado_tierra_del_fuego=sql^"""SELECT * FROM maestro_pozos AS m 
                            INNER JOIN prov_geojson AS p ON p.geojson=m.geojson 
                            WHERE provincia='Tierra del Fuego'"""
#40 pozos duplicados
años_produccion=sql^"""SELECT DISTINCT anio FROM pozos_no_convencional ORDER BY ANIO ASC"""
produccion_pozos_duplicados=sql^"""SELECT * FROM produccion_2024 
                                WHERE idpozo=29021 or idpozo=129805"""


#%%
import seaborn as sns
import matplotlib.pyplot as plt

# Crear el boxplot
sns.boxplot(x='cuenca', y='prod_pet', data=produccion_2024)

# Personalizar el gráfico
plt.title('Boxplot por Categorías')
plt.xticks(rotation=30, ha='right', fontsize=10)
plt.xlabel('Categorías')
plt.ylabel('Valores')
plt.tight_layout()
plt.show()

#%% la tabla maestro pozos contiene todos los pozos convencionales y no convencionales
id_maestro=sql^"""SELECT DISTINCT idpozo from maestro_pozos"""
id_convencional=sql^"""SELECT DISTINCT idpozo from produccion_2024"""
id_no_convencional=sql^"""SELECT DISTINCT idpozo from pozos_no_convencional"""


maestros_convencional=sql^ """SELECT DISTINCT idpozo from id_convencional 
                             EXCEPT
                             SELECT DISTINCT idpozo from id_maestro"""
#Hay 55 pozos convencionales que no estan en la tabla maestra
maestros_no_convencional=sql^ """SELECT DISTINCT idpozo from id_no_convencional 
                             EXCEPT
                             SELECT DISTINCT idpozo from id_maestro"""
#Hay 3 pozos no convencionales que no estan en la tabla maestra
convencional_no_convencional=sql^"""SELECT DISTINCT idpozo from id_no_convencional 
                             INTERSECT
                             SELECT DISTINCT idpozo from id_convencional"""
#Hay 4189 pozos catalogados como convencional y no convencional
#%%
pozosxcuenca=sql^"""SELECT DISTINCT cuenca, count(idpozo) AS pozos FROM maestro_pozos GROUP BY cuenca"""
pozosxcuenca_convencional=sql^"""SELECT DISTINCT cuenca, count(DISTINCT idpozo) AS pozos FROM produccion_2024 GROUP BY cuenca"""
pozosxcuenca_noconvencional=sql^"""SELECT DISTINCT cuenca, count(DISTINCT idpozo) AS pozos FROM pozos_no_convencional GROUP BY cuenca"""
id_empresa_conv=sql^ """SELECT DISTINCT idempresa,count(DISTINCT idpozo) AS pozos FROM produccion_2024 GROUP BY idempresa ORDER BY pozos DESC"""
id_empresa_noconv=sql^ """SELECT DISTINCT idempresa,count(DISTINCT idpozo) AS pozos FROM pozos_no_convencional GROUP BY idempresa ORDER BY pozos DESC"""

#%%Convencional
top_5_petroleo=sql^"""SELECT distinct idpozo FROM produccion_2024 ORDER BY prod_pet DESC LIMIT 5"""
top_5_gas=sql^"""SELECT distinct idpozo FROM produccion_2024 ORDER BY prod_gas DESC LIMIT 5"""

registros_top_5_petroleo=produccion_2024[(produccion_2024["idpozo"]==164827) |( produccion_2024["idpozo"]==165174 )| (produccion_2024["idpozo"]==165175) | (produccion_2024["idpozo"]==165173) | (produccion_2024["idpozo"]==165172)]

import seaborn as sns
import matplotlib.pyplot as plt

# Crear el boxplot
sns.lineplot(data=registros_top_5_petroleo, x="mes", y="prod_pet", hue='idpozo', marker='o',palette='bright')

# Personalizar el gráfico
plt.title('Ranking 5 pozos con mayor produccion de petroleo')
plt.xlabel('Meses del año 2024')
plt.ylabel('Producción')
plt.legend(title='idpozo')
plt.grid(True)
plt.show()

#%%Analisis petroleo convencional por cuenca
petroleo=sql^"""SELECT anio,mes, cuenca, sum(prod_pet) as produccion 
                FROM produccion_2024
                WHERE tipopozo='Petrolífero'
                GROUP BY anio,mes,cuenca HAVING produccion>0 """


import seaborn as sns
import matplotlib.pyplot as plt

# Crear el boxplot
sns.boxplot(x='cuenca', y='produccion', data=petroleo)

# Personalizar el gráfico
plt.title('Produccion de petroleo por cuencas 2024 meses 1-10')
plt.xticks(rotation=30, ha='right', fontsize=10)
plt.xlabel('Cuencas')
plt.ylabel('Produccion')
plt.tight_layout()
plt.show()



#%% esta bien ubicadas las cuencas?
import json
from shapely.geometry import shape
info_cuencas=pd.read_csv(carpeta+"exploracin-hidrocarburos-cuencas-sedimentarias.csv")
info_cuencas.iloc[17,0]="AUSTRAL"
info_cuencas.iloc[20,0]="GENERAL LEVALLE"
info_cuencas.iloc[2,0]="LOS BOLSONES"

cuencas=sql^"""SELECT DISTINCT cuenca FROM maestro_pozos"""
info_cuencas=sql^"""SELECT DISTINCT c.cuenca, tipo ,geojson FROM cuencas AS c 
                   INNER JOIN info_cuencas AS i ON i.cuenca=c.cuenca"""
info_cuencas["geojson"]=info_cuencas["geojson"].apply(json.loads)
info_cuencas["geojson"]=info_cuencas["geojson"].apply(shape)
                   
cuencasnull=sql^"""SELECT DISTINCT * FROM produccion_2024 WHERE cuenca IS NULL"""
#%%



#%%  COSAS FRANQUITO
import pandas as pd
from inline_sql import sql, sql_val
import matplotlib.pyplot as plt 
from   matplotlib import ticker 
import numpy as np

carpeta = "D:/FRAN/Educacion/UBA/Calidad_de_Datos/TP2/";

cap_pozos = pd.read_csv(carpeta+"capitulo-iv-pozos.csv");

prod_convencional = pd.read_csv(carpeta+"produccin-de-pozos-de-gas-y-petrleo-2024.csv");

prod_no_convencional = pd.read_csv(carpeta+"produccin-de-pozos-de-gas-y-petrleo-no-convencional.csv");

#%%

# Trabajo con la tabla prod_convencional

cant_id = sql^ """
                        SELECT COUNT(DISTINCT idpozo)
                        FROM prod_convencional
                    """ 
                    
#%%

tipoestados = sql^ """ SELECT DISTINCT tipoestado
                              FROM prod_convencional;
                          """ 
                          
#%%

produccion_cero = sql^ """
                        SELECT *
                        FROM prod_convencional
                        where prod_pet = 0 AND prod_gas = 0 AND (tipoextraccion NOT LIKE '%%Sin Sistema de Extracción%%' AND tipo_de_recurso LIKE '%%SIN RESERVORIO%%'
                         AND tipoestado NOT IN ('A Abandonar', 'Abandonado', 'Abandono Temporario', 'En Espera de Reparación', 'En Estudio', 'En Reparación', 
                        'Mantenimiento de Presión', 'Otras Situación Inactivo', 'Parado Alta Relación Agua/Petróleo', 'Parado Alta Relación Gas/Petróleo', 'Parado Transitoriamente'))
                    """ 
#%%                    
ids_prod_cero = sql^ """ SELECT idpozo, COUNT(*) AS veces
                         FROM produccion_cero
                         GROUP BY idpozo
                         HAVING COUNT(*) = 10;
                     """    

#%%

# Trabajo con la tabla prod_no_convencional

no_conv_cant_id = sql^ """
                        SELECT COUNT(DISTINCT idpozo)
                        FROM prod_no_convencional
                    """ 
                    
#%%

no_conv_extracciones = sql^ """ SELECT DISTINCT tipoextraccion
                   FROM prod_no_convencional;
               """ 
#%%

no_conv_estados = sql^ """ SELECT DISTINCT tipoestado
                           FROM prod_no_convencional;
                       """ 
                    
#%%

no_conv_recursos = sql^ """ SELECT DISTINCT tipo_de_recurso
                           FROM prod_no_convencional;
                       """   
                       
#%%

no_conv_produccion_cero = sql^ """
                        SELECT *
                        FROM prod_no_convencional
                        where prod_pet = 0 AND prod_gas = 0 AND (tipoextraccion NOT LIKE '%%Sin Sistema de Extracción%%'
                        AND tipoestado NOT IN ('A Abandonar', 'Abandonado', 'Abandono Temporario', 'En Espera de Reparación', 
                        'En Estudio', 'En Reparación', 'Mantenimiento de Presión', 'Otras Situación Inactivo', 
                        'Parado Alta Relación Agua/Petróleo', 'Parado Alta Relación Gas/Petróleo', 'Parado Transitoriamente'))
                        AND anio = 2024;
                    """      

#%%                    
no_conv_ids_prod_cero = sql^ """ SELECT idpozo, COUNT(*) AS veces
                         FROM no_conv_produccion_cero
                         GROUP BY idpozo
                         HAVING COUNT(*) = 10;
                     """                                                           
#%% los pozos estan en su respectiva provincia
import pandas as pd
from shapely.geometry import shape, Point
import json
esta_adentro=[]
maestro_pozos=separador_de_coordendas(maestro_pozos)
#%%
esta_adentro=[]
for index, row in  maestro_pozos.iterrows():
    for indice, fila in info_cuencas.iterrows():
        if row[8]==fila[0]: #Compara las cuencas
            esta_adentro.append(fila[2].contains(Point(row[27],row[26])))           
#%%
esta_adentro.count(False)
#hay 76 pozos que no estan en la cuenca que dicen estar
#%% problema provincia
info_provincias=pd.read_csv(carpeta+"provincia.csv")
#%%
from shapely.wkt import loads
id_geo=sql^"""SELECT DISTINCT idpozo,provincia,latitud,longitud FROM maestro_pozos"""
esta_adentro=[]
provincia_poligono=sql^"""SELECT DISTINCT nam,geom FROM info_provincias"""
provincia_poligono["geom"]=provincia_poligono["geom"].apply(loads)
prov_pol={}
for index,row in provincia_poligono.iterrows():
    if row[0]=="Río Negro":
        prov_pol["Rio Negro"]=row[1]
    elif row[0]=='Tierra del Fuego, Antártida e Islas del Atlántico Sur':
        prov_pol['Tierra del Fuego']=row[1]        
    else:   
        prov_pol[row[0]]=row[1]
        

#%%
for index, row in  id_geo.iterrows():
    if row[1]!="Estado Nacional":
        esta_adentro.append(prov_pol[row[1]].contains(Point(row[3],row[2]))) 
# 550 fuera de provincia, de las cuales 477 estan mal, ya que 73 son Estado nacional
#80 son repeticiones bah 40
#%%
no_conv_ids_prod_cero = sql^ """ SELECT COUNT(provincia) AS veces
                         FROM maestro_pozos
                         WHere provincia='Estado Nacional'"""

#%%
cantidad_provincias_repetidas=sql^ """SELECT DISTINCT geojson, count(DISTINCT provincia) as veces FROM maestro_pozos
                                      GROUP BY geojson HAVING veces>1   """

#%%

pozos=sql^ """SELECT idpozo, provincia,latitud,longitud FROM maestro_pozos AS m 
         INNER JOIN cantidad_provincias_repetidas AS c ON c.geojson = m.geojson"""

#%%
for index, row in pozos.iterrows():
    print(row[0],row[1],prov_pol[row[1]].contains(Point(row[3],row[2])))

#%% 
geojson_repetidos_en_cuencas=sql^"""SELECT DISTINCT geojson, count( DISTINCT cuenca) as veces FROM maestro_pozos
                                    GROUP BY geojson """
                                    
#%% K gradiente discreto
#produccion: Dataframe con los datos de producción
#tipo: petroleo o gas
#K_u: Factor superior
#K_l: Factor inferior
def k_metrica_gradiente_discreto(produccion,tipo,k_u,k_l):
    #ordena el dataset
    produccion=sql^"""SELECT * FROM produccion ORDER BY idpozo ASC, mes ASC, Anio Desc"""
    id_anomalo=[]
    mes_anomalo=[]
    descripcion=[]
    gradiente_previo=0
    primer_mes_id=True
    for i in range(len(produccion)-1):
        
        if produccion["idpozo"][i]==produccion["idpozo"][i+1] and primer_mes_id:
            gradiente_previo=abs(produccion[tipo][i+1]-produccion[tipo][i])
            primer_mes_id=False
        elif produccion["idpozo"][i]==produccion["idpozo"][i+1] and not( primer_mes_id): #tratamiento adentro de cada id
            
            if produccion[tipo][i]-gradiente_previo*k_l>produccion[tipo][i+1]:
                id_anomalo.append(produccion["idpozo"][i])
                mes_anomalo.append(produccion["mes"][i])
                if tipo=="prod_pet": 
                    descripcion.append(f"En el año: {produccion['anio'][i]} y en mes: {produccion['mes'][i]} se produjo menos petróleo de lo esperado")
                else:
                    descripcion.append(f"En el año: {produccion['anio'][i]} y en mes: {produccion['mes'][i]} se produjo menos gas de lo esperado")
                gradiente_previo=abs(produccion[tipo][i+1]-produccion[tipo][i])
                
            elif produccion[tipo][i]+gradiente_previo*k_u<produccion[tipo][i+1]:
                id_anomalo.append(produccion["idpozo"][i])
                if tipo=="prod_pet": 
                    descripcion.append(f"En el año: {produccion['anio'][i]} y en mes: {produccion['mes'][i]} se produjo más petróleo de lo esperado")
                else:
                    descripcion.append(f"En el año: {produccion['anio'][i]} y en mes: {produccion['mes'][i]} se produjo más  gas de lo esperado")
                gradiente_previo=abs(produccion[tipo][i+1]-produccion[tipo][i])
                
        else:
            primer_mes_id=True
    casos_anomalos={"idpozo":id_anomalo,"descripcion":descripcion} 
    casos_anomalos=pd.DataFrame(casos_anomalos)
    return casos_anomalos #Dataframe con los casos anomalos almacenados por pozo y año

produccion_conv_pet_anomala= k_metrica_gradiente_discreto(produccion_2024,"prod_pet",3,3)
#%%
produccion_conv_gas_anomala= k_metrica_gradiente_discreto(produccion_2024,"prod_gas",3,3)
#%%
pozos_no_convencional=sql^"""SELECT * FROM pozos_no_convencional WHERE anio=2024"""
produccion_no_conv_pet_anomala= k_metrica_gradiente_discreto(pozos_no_convencional,"prod_pet",3,3)
#%%
produccion_no_conv_gas_anomala= k_metrica_gradiente_discreto(pozos_no_convencional,"prod_gas",3,3)
#%%
coso_final=pd.concat([produccion_conv_pet_anomala,produccion_conv_gas_anomala,produccion_no_conv_pet_anomala,produccion_no_conv_gas_anomala])
#%%
coso_final.to_csv("casos_anomalos_de_produccion.csv")
#%%

"""cont_casos_normales=0
cont_mayor_prod=0
cont_menor_prod=0
for i in a.keys():
    for j in range(len(a[i])):
        if a[i][j][0]==False:
            cont_casos_normales+=1
        elif a[i][j][1]=="Cayó más de lo esperado":
            cont_menor_prod+=1
        else:
             cont_mayor_prod+=1
total=cont_casos_normales+cont_mayor_prod+cont_menor_prod
print("casos normales= ",cont_casos_normales/total*100)
print("Mayor prod= ",cont_mayor_prod/total*100)
print("Menor prod= ",cont_menor_prod/total*100)"""
cont_casos_normales=0
cont_mayor_prod=0
cont_menor_prod=0
for i in b.keys():
    for j in range(len(b[i])):
        if b[i][j][0]==False:
            cont_casos_normales+=1
        elif b[i][j][1]=="Cayó más de lo esperado":
            cont_menor_prod+=1
        else:
             cont_mayor_prod+=1
total=cont_casos_normales+cont_mayor_prod+cont_menor_prod
print("casos normales= ",cont_casos_normales/total*100)
print("Mayor prod= ",cont_mayor_prod/total*100)
print("Menor prod= ",cont_menor_prod/total*100)


#scoring
#me dice anomalo bueno 2 punto
#si produjo un mes que no escavado el pozo 7
#Si produjo negativo 10 puntos
#Si sumaste mas de 7 sos anomalo




#%% scoring
import json
from shapely.geometry import shape
info_cuencas=pd.read_csv(carpeta+"exploracin-hidrocarburos-cuencas-sedimentarias.csv")
info_cuencas.iloc[17,0]="AUSTRAL"
info_cuencas.iloc[20,0]="GENERAL LEVALLE"
info_cuencas.iloc[2,0]="LOS BOLSONES"

cuencas=sql^"""SELECT DISTINCT cuenca FROM maestro_pozos"""
info_cuencas=sql^"""SELECT DISTINCT c.cuenca, tipo ,geojson FROM cuencas AS c 
                   INNER JOIN info_cuencas AS i ON i.cuenca=c.cuenca"""
info_cuencas["geojson"]=info_cuencas["geojson"].apply(json.loads)
info_cuencas["geojson"]=info_cuencas["geojson"].apply(shape)
#%%                   
info_provincias=pd.read_csv(carpeta+"provincia.csv")
maestro_pozos=separador_de_coordendas(maestro_pozos)
#%%
from shapely.wkt import loads
from shapely.geometry import shape, Point
id_geo=sql^"""SELECT DISTINCT idpozo,provincia,latitud,longitud FROM maestro_pozos"""
id_equivocado=[]
descripcion=[]
provincia_poligono=sql^"""SELECT DISTINCT nam,geom FROM info_provincias"""
provincia_poligono["geom"]=provincia_poligono["geom"].apply(loads)
prov_pol={}
for index,row in provincia_poligono.iterrows():
    if row[0]=="Río Negro":
        prov_pol["Rio Negro"]=row[1]
    elif row[0]=='Tierra del Fuego, Antártida e Islas del Atlántico Sur':
        prov_pol['Tierra del Fuego']=row[1]        
    else:   
        prov_pol[row[0]]=row[1]
#%%
id_equivocado=[]
descripcion=[]
for i in range(len(id_geo)):
    if id_geo["provincia"][i]!="Estado Nacional":
        if not((prov_pol[id_geo["provincia"][i]]).contains(Point(id_geo["longitud"][i],id_geo["latitud"][i]))):
            id_equivocado.append(id_geo["idpozo"][i])
            descripcion.append("Provincia incosistentes con el geojson")
#%%
provincias_anomalas=pd.DataFrame({"idpozo":id_equivocado,"descripcion":descripcion})
#%%
provincias_anomalas.to_csv("Provincias_anomalas.csv")
#%%
datos_2023=pd.read_csv("C:\\Users\\emili\\OneDrive\\Desktop\\datos_tp_final_cdl\\produccin-de-pozos-de-gas-y-petrleo-2023.csv")
#%%
ids_viejos=sql^"""SELECT DISTINCT idpozo FROM datos_2023
                INTERSECT
                 SELECT DISTINCT idpozo FROM produccion_2024"""
#%%
produccion_diciembre_2023=sql^"""SELECT DISTINCT idpozo,prod_pet,prod_gas,anio,mes FROM datos_2023
                                 WHERE idpozo IN (SELECT idpozo FROM ids_viejos) AND mes=12"""

#%%
def k_metrica_gradiente_discreto_ampliado(produccion,tipo,k_u,k_l):
    #ordena el dataset
    ids_viejos=sql^"""SELECT DISTINCT idpozo FROM datos_2023
                      INTERSECT
                      SELECT DISTINCT idpozo FROM produccion"""
    datos_nov_dic_2023=sql^"""SELECT DISTINCT d.idpozo,d.prod_pet,d.prod_gas,d.anio,d.mes FROM datos_2023 as d
                          WHERE 
                          d.idpozo IN (SELECT idpozo FROM ids_viejos) AND mes=12 """
                              
    produccion=sql^"""SELECT idpozo,prod_pet,prod_gas,anio,mes FROM produccion 
                      UNION
                      SELECT * FROM datos_nov_dic_2023
                      """
    produccion=sql^"""SELECT idpozo,prod_pet,prod_gas,anio,mes FROM produccion ORDER BY idpozo ASC, Anio ASC, mes ASC"""
    
    id_anomalo=[]
    tipo_anomalo=[]
    mes_anomalo=[]
    descripcion=[]
    gradiente_previo=0
    primer_mes_id=True
    for i in range(len(produccion)-1):
        
        if produccion["idpozo"][i]==produccion["idpozo"][i+1] and primer_mes_id:
            gradiente_previo=abs(produccion[tipo][i+1]-produccion[tipo][i])
            primer_mes_id=False
        elif produccion["idpozo"][i]==produccion["idpozo"][i+1] and not( primer_mes_id): #tratamiento adentro de cada id
            
            if produccion[tipo][i]-gradiente_previo*k_l>produccion[tipo][i+1]:
                id_anomalo.append(produccion["idpozo"][i])
                mes_anomalo.append(produccion["mes"][i])
                if tipo=="prod_pet": 
                    descripcion.append(f"En el año: {produccion['anio'][i]} y en mes: {produccion['mes'][i]} se produjo menos petróleo de lo esperado")
                else:
                    descripcion.append(f"En el año: {produccion['anio'][i]} y en mes: {produccion['mes'][i]} se produjo menos gas de lo esperado")
                gradiente_previo=abs(produccion[tipo][i+1]-produccion[tipo][i])
                
            elif produccion[tipo][i]+gradiente_previo*k_u<produccion[tipo][i+1]:
                id_anomalo.append(produccion["idpozo"][i])
                if tipo=="prod_pet": 
                    descripcion.append(f"En el año: {produccion['anio'][i]} y en mes: {produccion['mes'][i]} se produjo más petróleo de lo esperado")
                else:
                    descripcion.append(f"En el año: {produccion['anio'][i]} y en mes: {produccion['mes'][i]} se produjo más  gas de lo esperado")
                gradiente_previo=abs(produccion[tipo][i+1]-produccion[tipo][i])
                
        else:
            primer_mes_id=True
    casos_anomalos={"idpozo":id_anomalo,"descripcion":descripcion} 
    casos_anomalos=pd.DataFrame(casos_anomalos)
    return casos_anomalos #Dataframe con los casos anomalos almacenados por pozo y año
#%%
produccion_conv_pet_anomala= k_metrica_gradiente_discreto_ampliado(produccion_2024,"prod_pet",3,3)
#%%
produccion_conv_gas_anomala= k_metrica_gradiente_discreto_ampliado(produccion_2024,"prod_gas",3,3)
#%%
pozos_no_convencional=sql^"""SELECT * FROM pozos_no_convencional WHERE anio=2024"""
produccion_no_conv_pet_anomala= k_metrica_gradiente_discreto_ampliado(pozos_no_convencional,"prod_pet",3,3)
#%%
produccion_no_conv_gas_anomala= k_metrica_gradiente_discreto_ampliado(pozos_no_convencional,"prod_gas",3,3)
#%%
coso_final=pd.concat([produccion_conv_pet_anomala,produccion_conv_gas_anomala,produccion_no_conv_pet_anomala,produccion_no_conv_gas_anomala])
#%%
coso_final.to_csv("casos_anomalos_de_produccion.csv")
#%%
traka=pd.read_csv("C:\\Users\\emili\\OneDrive\\Desktop\\casos_anomalos_de_produccion.csv")


    



