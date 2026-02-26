import pandas as pd
from dagster import asset, asset_check, AssetCheckResult, MetadataValue
from plotnine import *

@asset
def renta_raw():
    """Carga del dataset original"""
    df = pd.read_csv('distribucion-renta-canarias.csv')

    # ROMPER CHECK: Descomentar la siguiente línea para romper check_nulos_numericos:
    # df.loc[0, 'OBS_VALUE'] = None

    return df

@asset_check(asset=renta_raw)
def check_nulos_numericos(renta_raw):
    """Valida nulos en los futuros ejes X (Año) e Y (Valor)"""
    nulos_obs = int(renta_raw['OBS_VALUE'].isna().sum())
    nulos_time = int(renta_raw['TIME_PERIOD#es'].isna().sum())
    
    passed = (nulos_obs == 0) and (nulos_time == 0)
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "nulos_valor_renta": MetadataValue.int(nulos_obs),
            "nulos_año": MetadataValue.int(nulos_time),
            "principio_gestalt": "Figura y Fondo / Continuidad",
            "impacto": "Los nulos en OBS_VALUE o TIME_PERIOD impiden dibujar la línea del tiempo."
        }
    )

@asset_check(asset=renta_raw)
def check_estandarizacion_categorias(renta_raw):
    """Valida consistencia en Nombres de Islas (facetas) y Tipos de Renta (líneas)"""
    # Islas (Evitar "Tenerife " con espacio o "tenerife" en minúscula)
    originales_islas = int(renta_raw['TERRITORIO#es'].nunique())
    limpias_islas = int(renta_raw['TERRITORIO#es'].str.strip().str.capitalize().nunique())
    
    # Medidas (Asegurar que solo existen las categorías esperadas)
    esperadas = {'Sueldos y salarios', 'Pensiones', 'Prestaciones por desempleo', 'Rentas de actividades económicas', 'Otras prestaciones', 'Otros ingresos'}
    reales = set(renta_raw['MEDIDAS#es'].unique())
    diff = len(reales - esperadas)
    
    passed = (originales_islas == limpias_islas) and (diff == 0)
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "error_formato_islas": MetadataValue.bool(originales_islas != limpias_islas),
            "categorias_medida_extra": MetadataValue.int(diff),
            "principio_gestalt": "Similitud",
            "impacto": "Inconsistencias crean leyendas duplicadas o cuadros (facets) vacíos."
        }
    )

@asset
def renta_clean(renta_raw):
    """Limpieza de datos: nulos, columnas extra y renombramiento"""
    df = renta_raw.copy()

    # Limpieza de nulos en la columna de valor -> se puede mover arriba, antes del check
    df = df.dropna(subset=['OBS_VALUE'])
    
    # Eliminación de columnas técnicas
    df = df.drop(columns=['ESTADO_OBSERVACION#es', 'CONFIDENCIALIDAD_OBSERVACION#es'])
    
    # Renombrado para facilitar el uso en Plotnine
    df = df.rename(columns={
        'TERRITORIO#es': 'TERRITORIO',
        'TIME_PERIOD#es': 'TIME_PERIOD',
        'MEDIDAS#es': 'MEDIDAS',
        'TERRITORIO_CODE': 'COD_MUN' # Preparar para pasos siguientes..
    })
    
    # ROMPER CHECK: Descomentar la siguiente línea para romper check_cardinalidad_islas:
    # df = df[df['TERRITORIO#es'] != 'Tenerife']

    # ROMPER CHECK: Descomentar la siguiente línea para romper check_rangos_renta:
    # df.loc[1, 'OBS_VALUE'] = 999.9 

    return df

@asset_check(asset=renta_clean)
def check_cardinalidad_islas(renta_clean):
    """Valida que el número de islas sea el esperado tras la limpieza"""
    lista_esperada = {'Lanzarote', 'Fuerteventura', 'Gran Canaria', 'Tenerife', 'La Gomera', 'La Palma', 'El Hierro'}
    islas_reales = set(renta_clean[renta_clean['TERRITORIO'].isin(lista_esperada)]['TERRITORIO'].unique())
    
    passed = len(islas_reales) == 7
    return AssetCheckResult(
        passed=passed,
        metadata={
            "n_islas": MetadataValue.int(len(islas_reales)),
            "principio_gestalt": "Carga Cognitiva",
            "impacto": "Evita que el facet_wrap sea incompleto."
        }
    )

@asset_check(asset=renta_clean)
def check_rangos_renta(renta_clean):
    """Verifica que los porcentajes de renta sean realistas (0-100) para comprobar outliers"""
    fuera_de_rango = renta_clean[(renta_clean['OBS_VALUE'] < 0) | (renta_clean['OBS_VALUE'] > 100)]
    
    passed = len(fuera_de_rango) == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "casos_anomalos": MetadataValue.int(len(fuera_de_rango)),
            "principio_gestalt": "Veracidad Visual",
            "impacto": "Valores > 100 comprimen la escala del eje Y, haciendo invisibles las diferencias reales."
        }
    )

@asset_check(asset=renta_clean)
def check_continuidad_temporal(renta_clean):
    """Continuidad: Verifica que no falten años en la serie para evitar pendientes falsas."""
    años_presentes = sorted(renta_clean['TIME_PERIOD'].unique())
    
    rango_teorico = list(range(int(min(años_presentes)), int(max(años_presentes)) + 1))
    
    faltantes = set(rango_teorico) - set(años_presentes)
    passed = len(faltantes) == 0
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "años_faltantes": MetadataValue.text(str(list(faltantes))),
            "principio_gestalt": "Continuidad",
            "impacto": "Años faltantes crean líneas rectas que ocultan la realidad histórica."
        }
    )


@asset
def grafico_evolucion_islas(renta_clean):
    """Generación del gráfico de líneas"""
    lista_islas = [
        'Lanzarote', 'Fuerteventura', 'Gran Canaria', 
        'Tenerife', 'La Gomera', 'La Palma', 'El Hierro'
    ]
    
    # Filtrar solo islas para este gráfico inicial
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
    
    # Se guarda el resultado físicamente
    grafico.save("grafico_lineas_renta_islas_2.png")
    return grafico


@asset_check(asset=grafico_evolucion_islas)
def check_mapeo_color_en_grafico(grafico_evolucion_islas):
    """Verifica que el gráfico tenga correctamente definido el mapeo de color y que esté basado en MEDIDAS."""
    # Verificar que existe mapeo de color
    mapping = grafico_evolucion_islas.mapping
    color_mapping = mapping.get("color", None)

    tiene_color = color_mapping is not None
    color_es_medidas = str(color_mapping) == "MEDIDAS"

    # Verificar número de categorías visualizadas
    categorias = set(grafico_evolucion_islas.data["MEDIDAS"].unique())
    n_categorias = len(categorias)

    passed = tiene_color and color_es_medidas and n_categorias > 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "tiene_mapeo_color": MetadataValue.bool(tiene_color),
            "n_categorias_en_plot": MetadataValue.int(n_categorias),
            "principio_gestalt": "Similitud",
            "impacto": "Si el color no está mapeado a MEDIDAS, se pierde diferenciación visual entre tipos de renta."
        }
    )  

# --- ASSETS DE CÓDIGOS ---
@asset
def codigos_geograficos():
    """Asset que carga y prepara el diccionario de municipios"""
    df_cods = pd.read_csv('codislas.csv', encoding='iso-8859-1', sep=';')
    
    # ROMPER CHECK: Descomentar para romper check_integridad_codigos:
    # df_cods.loc[0, 'CMUN'] = 99999999

    # Construcción del código de 5 dígitos
    df_cods['COD_MUN'] = (
        df_cods['CPRO'].astype(str).str.zfill(2) + 
        df_cods['CMUN'].astype(str).str.zfill(3)
    ).str.strip()
    
    return df_cods[['COD_MUN', 'NOMBRE', 'ISLA']]

@asset_check(asset=codigos_geograficos)
def check_integridad_codigos(codigos_geograficos):
    """Valida que los códigos de municipio tengan el formato correcto de 5 dígitos"""
    # Verificar longitud y que no haya nulos en la llave de unión
    longitudes = codigos_geograficos['COD_MUN'].str.len().unique()
    nulos_cod = int(codigos_geograficos['COD_MUN'].isna().sum())
    
    passed = (len(longitudes) == 1 and longitudes[0] == 5) and (nulos_cod == 0)
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "longitudes_detectadas": MetadataValue.text(str(list(longitudes))),
            "nulos_en_llave": MetadataValue.int(nulos_cod),
            "principio_gestalt": "Similitud",
            "impacto": "Códigos mal formados impedirán el merge con los datos de renta."
        }
    )


@asset
def renta_municipios_unificada(renta_clean, codigos_geograficos):
    """Une la renta limpia con los nombres de municipios y aplica ordenación por sueldo"""
    df_renta = renta_clean.copy()

    # Limpieza específica para el error detectado (-> Informe)
    df_renta['COD_MUN'] = df_renta['COD_MUN'].str.split('_').str[0]

    df_renta['COD_MUN'] = df_renta['COD_MUN'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    # Filtro de año (usar 2022 como en el prototipo)
    df_renta_filt = df_renta[df_renta['TIME_PERIOD'].astype(str) == '2022'].copy()
    
    df_final = pd.merge(df_renta_filt, codigos_geograficos, on='COD_MUN')
    
    # LÓGICA DE ORDENACIÓN: Pre-calculamos el orden basado en 'Sueldos y salarios'
    df_sueldos = df_final[df_final['MEDIDAS'] == 'Sueldos y salarios'][['NOMBRE', 'OBS_VALUE']]
    df_sueldos = df_sueldos.rename(columns={'OBS_VALUE': 'valor_referencia'})
    
    df_final = pd.merge(df_final, df_sueldos, on='NOMBRE', how='left')
    
    # Convertir NOMBRE en categoría ordenada (el valor más alto arriba en coord_flip)
    df_final['NOMBRE'] = pd.Categorical(
        df_final['NOMBRE'], 
        categories=df_final.sort_values('valor_referencia', ascending=True)['NOMBRE'].unique(),
        ordered=True
    )
    
    return df_final


@asset_check(asset=renta_municipios_unificada)
def check_exito_unificacion(renta_municipios_unificada):
    """Verifica que el merge no haya dejado municipios sin isla asignada"""
    n_sin_isla = int(renta_municipios_unificada['ISLA'].isna().sum())
    # Verificar que tras el merge están los 88 municipios de Canarias
    n_municipios = int(renta_municipios_unificada['NOMBRE'].nunique())
    
    passed = n_sin_isla == 0 and n_municipios == 88
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "municipios_detectados": MetadataValue.int(n_municipios),
            "filas_sin_isla": MetadataValue.int(n_sin_isla),
            "principio_gestalt": "Figura y Fondo",
            "impacto": "Si falla el merge, los municipios desaparecerán del gráfico (fondo vacío)."
        }
    )

@asset_check(asset=renta_municipios_unificada)
def check_orden_por_sueldos(renta_municipios_unificada):
    """Valida que el orden de los nombres coincida con la magnitud del valor_referencia."""
    nombres_ordenados = renta_municipios_unificada['NOMBRE'].cat.categories
    
    mapeo_valores = renta_municipios_unificada.set_index('NOMBRE')['valor_referencia'].to_dict()
    valores_en_orden = [mapeo_valores[n] for n in nombres_ordenados]
    
    # VERIFICACIÓN ¿Están los valores en orden ascendente o descendente?
    es_ascendente = all(valores_en_orden[i] <= valores_en_orden[i+1] for i in range(len(valores_en_orden)-1))
    
    passed = es_ascendente
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "n_municipios": MetadataValue.int(len(nombres_ordenados)),
            "es_orden_ascendente": MetadataValue.bool(es_ascendente),
            "principio_gestalt": "Continuidad",
            "impacto": "Si falla, las barras del gráfico aparecerán desordenadas, rompiendo la jerarquía visual."
        }
    )


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
    
    grafico.save("grafico_barras_municipios.png", limitsize=False)
    return "Gráfico generado con éxito"


# --- ASSETS DE ESTUDIOS ---
@asset
def estudios_raw():
    """Carga del dataset de nivel de estudios"""
    return pd.read_csv('nivelestudios.csv')

@asset
def estudios_unificados_islas(estudios_raw, codigos_geograficos):
    """Limpieza de estudios y unión con la información de ISLA de codigos_geograficos"""
    df_est = estudios_raw.copy()
    
    # 1. Limpieza y filtrado inicial
    df_est = df_est[
        (df_est['Sexo'].str.contains('Hombres|Mujeres', case=False, na=False)) & 
        (df_est['Nivel de estudios en curso'] != 'Total') &
        (df_est['Nivel de estudios en curso'] != 'No cursa estudios')
    ].copy()
    
    # 2. Normalización del código de municipio (extraer primeros 5 caracteres)
    df_est['COD_MUN'] = df_est.iloc[:, 0].astype(str).str.strip().str[:5]
    
    # 3. Preparación de códigos geográficos (asegurar coincidencia de tipos)
    df_geo = codigos_geograficos.copy()
    df_geo['COD_MUN'] = df_geo['COD_MUN'].astype(str).str.strip()
    
    # 4. Merge para obtener la columna 'ISLA'
    df_unificado = pd.merge(df_est, df_geo[['COD_MUN', 'ISLA']], on='COD_MUN')
    
    # 5. Normalización del Año (extraer los dos últimos dígitos de 'Periodo')
    df_unificado['Año'] = "20" + df_unificado['Periodo'].str.extract(r'(\d{2})$').iloc[:, 0]
    
    # ROMPER CHECK: Descomendar para romper check_consistencia_anos:
    # df_unificado.loc[0, 'Año'] = "22"

    # ROMPER CHECK: Descomendar para romper check_contraste_sexo:
    # df_unificado = df_unificado[df_unificado['Sexo'] != 'Mujeres']

    return df_unificado

@asset_check(asset=estudios_unificados_islas)
def check_integridad_merge_islas(estudios_unificados_islas):
    """Valida que no se hayan perdido registros clave durante la unión geográfica"""
    # Figura y Fondo: Si ISLA es nulo, se vuelve "invisible" en el gráfico
    n_nulos_isla = int(estudios_unificados_islas['ISLA'].isna().sum())
    passed = n_nulos_isla == 0
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "n_filas_sin_isla": MetadataValue.int(n_nulos_isla),
            "principio_gestalt": "Figura y Fondo",
            "impacto": "Registros sin isla no aparecerán en las facetas del gráfico."
        }
    )

@asset_check(asset=estudios_unificados_islas)
def check_consistencia_anos(estudios_unificados_islas):
    """Verifica que el eje X (Año) tenga un formato uniforme."""
    años_detectados = estudios_unificados_islas['Año'].unique().tolist()
    
    # Verificar formato YYYY (4 dígitos empezando por 20)
    es_formato_correcto = all(str(a).startswith('20') and len(str(a)) == 4 for a in años_detectados)
    
    return AssetCheckResult(
        passed=es_formato_correcto,
        metadata={
            "años_en_datos": MetadataValue.text(str(años_detectados)),
            "principio_gestalt": "Similitud",
            "impacto": "Garantiza que el gráfico no mezcle formatos temporales en el eje X."
        }
    )

@asset_check(asset=estudios_unificados_islas)
def check_contraste_sexo(estudios_unificados_islas):
    """Valida la existencia de ambas categorías de Sexo en el DataFrame."""
    generos = estudios_unificados_islas['Sexo'].unique()

    passed = len(generos) == 2
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "categorias_en_datos": MetadataValue.int(len(generos)),
            "listado_sexos": MetadataValue.text(str(list(generos))),
            "principio_gestalt": "Semejanza",
            "impacto": "Si falta una categoría en los datos, el contraste de color en el gráfico desaparece."
        }
    )

@asset
def grafico_estudios_por_sexo(estudios_unificados_islas):
    """Generación del gráfico de barras agrupadas por Año y Sexo (Hito 7)"""
    df = estudios_unificados_islas
    
    if df.empty:
        return "Error: No hay datos tras el merge para generar el gráfico"
    
    # Agrupación de los totales
    df_plot = df.groupby(['Año', 'Sexo', 'ISLA'], as_index=False)['Total'].sum()
    
    grafico = (
        ggplot(df_plot, aes(x='Año', y='Total', fill='Sexo'))
        + geom_col(position='dodge')
        + facet_wrap('~ISLA', scales='free_y')
        + theme_minimal()
        + scale_fill_manual(values=['#4C72B0', '#DD8452'])
        + labs(
            title="Evolución de Estudiantes por Año y Sexo",
            subtitle="Análisis por Islas (2021-2023)",
            x="Año", 
            y="Nº de Estudiantes", 
            fill="Sexo"
        )
        + theme(
            figure_size=(14, 10), 
            strip_text=element_text(size=10, face='bold'),
            legend_position='bottom'
        )
    )
    
    grafico.save("grafico_barras_estudios_sexo.png")
    return "Gráfico de distribución de estudios por sexo generado con éxito"
