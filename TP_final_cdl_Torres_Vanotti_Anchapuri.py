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
#pozos_no_convencional=pd.read_csv(carpeta+"produccin-de-pozos-de-gas-y-petrleo-no-convencional.csv")
#produccion_2024=pd.read_csv(carpeta+"produccin-de-pozos-de-gas-y-petrleo-2024.csv")

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