# Prototipo para diseñar el gráfico de forma iterativa

import pandas as pd
from plotnine import *

# 1. --- CARGA DEL DATASET ---
df_renta = pd.read_csv('distribucion-renta-canarias.csv')

# 2. --- LIMPIEZA Y FILTRADO ---
# Comprobación de NaN/nulos
# Contamos cuántos nulos hay por columna
nulos = df_renta.isnull().sum()
print("Recuento de nulos por columna:\n", nulos)

# Acción correctiva: Si existieran nulos en OBS_VALUE, los eliminamos 
# para que plotnine no falle al dibujar las líneas
if df_renta['OBS_VALUE'].isnull().any():
    print("¡Atención! Se han detectado nulos. Limpiando...")
    df_renta = df_renta.dropna(subset=['OBS_VALUE'])

# Eliminamos las columnas de estado y confidencialidad que no aportan al gráfico
cols_a_eliminar = ['ESTADO_OBSERVACION#es', 'CONFIDENCIALIDAD_OBSERVACION#es']
df_renta = df_renta.drop(columns=cols_a_eliminar)

# Renombrar columnas para quitar los '#'
df_renta = df_renta.rename(columns={
    'TERRITORIO#es': 'TERRITORIO',
    'TIME_PERIOD#es': 'TIME_PERIOD',
    'MEDIDAS#es': 'MEDIDAS'
})
    
# Creamos una lista con los nombres exactos de las islas tal como aparecen en el CSV
lista_islas = [
    'Lanzarote', 'Fuerteventura', 'Gran Canaria', 
    'Tenerife', 'La Gomera', 'La Palma', 'El Hierro'
]

# Filtramos el DataFrame: solo filas donde el territorio esté en nuestra lista
df_islas = df_renta[df_renta['TERRITORIO'].isin(lista_islas)].copy()

# Convertimos el periodo de tiempo a numérico por si acaso
df_islas['TIME_PERIOD'] = pd.to_numeric(df_islas['TIME_PERIOD'])

# 3. --- PROTOTIPADO DE UN GRÁFICO DE LÍNEAS POR ISLA ---
grafico = (
    ggplot(df_islas, aes(x='TIME_PERIOD', y='OBS_VALUE', color='MEDIDAS', group='MEDIDAS'))
    + geom_line(size=1)
    + geom_point(size=2)
    + facet_wrap('~TERRITORIO') 
    + theme_minimal()
    + labs(
        title="Distribución de la Renta por Isla",
        x="Año",
        y="Valor (%)",
        color="Tipo de Renta"
    )
    + theme(figure_size=(12, 8), axis_text_x=element_text(rotation=45), strip_text=element_text(size=12, face="bold"))
)

grafico.save("grafico_lineas_renta_islas.png")