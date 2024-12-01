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
    dataframe["geojson"]=dataframe["geojson"].apply(loads)
    latitud=[]
    longitud=[]
    for index, _ in dataframe.iterrows():
        json=dataframe["geojson"].loc[index]
        latitud.append(json["coordinates"][1])
        longitud.append(json["coordinates"][0])
    
    coordenadas=pd.DataFrame({"latitud":latitud,"longitud":longitud})

    dataframe.drop("geojson",axis=1,inplace=True)

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
info_cuencas=pd.read_csv(carpeta+"exploracin-hidrocarburos-cuencas-sedimentarias.csv")
info_cuencas.iloc[17,0]="AUSTRAL"
info_cuencas.iloc[20,0]="GENERAL LEVALLE"
info_cuencas.iloc[2,0]="LOS BOLSONES"

cuencas=sql^"""SELECT DISTINCT cuenca FROM maestro_pozos"""
info_cuencas=sql^"""SELECT DISTINCT c.cuenca, tipo ,geojson FROM cuencas AS c 
                   INNER JOIN info_cuencas AS i ON i.cuenca=c.cuenca"""
cuencasnull=sql^"""SELECT DISTINCT * FROM produccion_2024 WHERE cuenca IS NULL"""
#%%
import geopandas as gpd
from shapely.geometry import Point
maestro_pozos=separador_de_coordendas(maestro_pozos)
correctamente_ubicado=[]
for i in range(len(maestro_pozos)):
    fila=maestro_pozos.iloc[i,:]
    punto = Point(maestro_pozos["latitud"],maestro_pozos["longitud"])  # Ejemplo: Córdoba, Argentina
    region= info_cuencas[:,2]
# Verificar si el punto está dentro de alguna región del GeoJSON
    esta_dentro = region.contains(punto).any()
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
    














