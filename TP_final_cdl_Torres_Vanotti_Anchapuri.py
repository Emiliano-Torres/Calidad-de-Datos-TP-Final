"""
@author: Emiliano Torres, Franco Vanotti, Juan Anchapuri
"""

#%% Importamos las librerias
import pandas as pd
import matplotlib.pyplot as plt
from inline_sql import sql,sql_val

#%% Importamos las bases de datos
#Emi "C:\\Users\\emili\\OneDrive\\Desktop\\Tp_final_cld\\"
carpeta=("C:\\Users\\emili\\OneDrive\\Desktop\\Tp_final_cld\\")
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
unicidad_maestro_pozos_geojson_check=sql^"""SELECT DISTINCT geojson, COUNT(DISTINCT idpozo) AS repeticiones from maestro_pozos
                                      GROUP BY geojson HAVING repeticiones>1 ORDER BY repeticiones desc """
