import pandas as pd
from dagster import asset
from plotnine import *

@asset
def renta_raw():
    """Carga del dataset original"""
    # Usamos el CSV original que ya conocemos
    return pd.read_csv('distribucion-renta-canarias.csv')

@asset
def renta_clean(renta_raw):
    """Limpieza de datos: nulos, columnas extra y renombramiento"""
    df = renta_raw.copy()
    
    # Limpieza de nulos en la columna de valor
    df = df.dropna(subset=['OBS_VALUE'])
    
    # Eliminación de columnas técnicas
    df = df.drop(columns=['ESTADO_OBSERVACION#es', 'CONFIDENCIALIDAD_OBSERVACION#es'])
    
    # Renombrado para facilitar el uso en Plotnine
    df = df.rename(columns={
        'TERRITORIO#es': 'TERRITORIO',
        'TIME_PERIOD#es': 'TIME_PERIOD',
        'MEDIDAS#es': 'MEDIDAS',
        'TERRITORIO_CODE': 'COD_MUN' # Lo preparamos para el punto 6
    })
    
    return df

@asset
def grafico_evolucion_islas(renta_clean):
    """Generación del gráfico de líneas (Punto 5)"""
    lista_islas = [
        'Lanzarote', 'Fuerteventura', 'Gran Canaria', 
        'Tenerife', 'La Gomera', 'La Palma', 'El Hierro'
    ]
    
    # Filtramos solo islas para este gráfico inicial
    df_islas = renta_clean[renta_clean['TERRITORIO'].isin(lista_islas)].copy()
    df_islas['TIME_PERIOD'] = pd.to_numeric(df_islas['TIME_PERIOD'])
    
    grafico = (
        ggplot(df_islas, aes(x='TIME_PERIOD', y='OBS_VALUE', color='MEDIDAS', group='MEDIDAS'))
        + geom_line(size=1)
        + geom_point(size=2)
        + facet_wrap('~TERRITORIO') 
        + theme_minimal()
        + labs(
            title="Distribución de la Renta por Isla",
            subtitle="Datos procesados mediante Assets de Dagster",
            x="Año",
            y="Valor (%)",
            color="Tipo de Renta"
        )
        + theme(figure_size=(12, 8), axis_text_x=element_text(rotation=45))
    )
    
    # Guardamos el resultado físicamente
    # grafico.save("grafico_lineas_renta_islas_2.png")
    
    # Dagster puede devolver el gráfico o simplemente una confirmación
    return "Gráfico de líneas generado con éxito"

