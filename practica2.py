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
    grafico.save("grafico_lineas_renta_islas_2.png")
    # Dagster puede devolver el gráfico o simplemente una confirmación
    return "Gráfico de líneas generado con éxito"


@asset
def codigos_geograficos():
    """Asset que carga y prepara el diccionario de municipios (Punto 6)"""
    df_cods = pd.read_csv('codislas.csv', encoding='iso-8859-1', sep=';')
    
    # Construcción robusta del código de 5 dígitos
    df_cods['COD_MUN'] = (
        df_cods['CPRO'].astype(str).str.zfill(2) + 
        df_cods['CMUN'].astype(str).str.zfill(3)
    ).str.strip()
    
    return df_cods[['COD_MUN', 'NOMBRE', 'ISLA']]

@asset
def renta_municipios_unificada(renta_clean, codigos_geograficos):
    """Une la renta limpia con los nombres de municipios y aplica ordenación por sueldo"""
    # Limpieza de códigos en renta_clean
    df_renta = renta_clean.copy()
    df_renta['COD_MUN'] = df_renta['COD_MUN'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    # Filtro de año (usamos 2022 como en el prototipo)
    df_renta_filt = df_renta[df_renta['TIME_PERIOD'].astype(str) == '2022'].copy()
    
    # Merge con el diccionario
    df_final = pd.merge(df_renta_filt, codigos_geograficos, on='COD_MUN')
    
    # LÓGICA DE ORDENACIÓN: Pre-calculamos el orden basado en 'Sueldos y salarios'
    df_sueldos = df_final[df_final['MEDIDAS'] == 'Sueldos y salarios'][['NOMBRE', 'OBS_VALUE']]
    df_sueldos = df_sueldos.rename(columns={'OBS_VALUE': 'valor_referencia'})
    
    df_final = pd.merge(df_final, df_sueldos, on='NOMBRE', how='left')
    
    # Convertimos NOMBRE en categoría ordenada (el valor más alto arriba en coord_flip)
    df_final['NOMBRE'] = pd.Categorical(
        df_final['NOMBRE'], 
        categories=df_final.sort_values('valor_referencia', ascending=True)['NOMBRE'].unique(),
        ordered=True
    )
    
    return df_final

@asset
def grafico_distribucion_municipios(renta_municipios_unificada):
    """Genera el gráfico de barras apiladas al 100% ordenado por sueldos"""
    df_plot = renta_municipios_unificada
    
    grafico = (
        ggplot(df_plot, aes(x='NOMBRE', y='OBS_VALUE', fill='MEDIDAS'))
        + geom_col(position='fill') 
        + coord_flip()
        + facet_wrap('~ISLA', scales='free_y')
        + scale_y_continuous(labels=lambda l: ["%d%%" % (v * 100) for v in l])
        + scale_fill_brewer(type='qual', palette='Set2') 
        + theme_minimal()
        + labs(
            title="Distribución de la Renta por Municipio (2022)",
            subtitle="Municipios ordenados por volumen de Sueldos y Salarios",
            x="Municipio",
            y="Proporción",
            fill="Origen de Renta"
        )
        + theme(
            figure_size=(15, 25),
            axis_text_y=element_text(size=11, face='bold', color='black'),
            legend_position='bottom',
            strip_text=element_text(size=14, face='bold')
        )
    )
    
    # Guardamos el archivo físico
    grafico.save("grafico_barras_municipios.png", limitsize=False)
    return "Gráfico de distribución por municipios generado con éxito"