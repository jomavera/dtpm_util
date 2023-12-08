import datetime
import os
import time

import pandas as pd

from dtpm_util.utilidades.preprocesamiento import concatenar_desde_carpeta
from dtpm_util.utilidades.excel.hoja_calculo import abrir_archivo
from dtpm_util.utilidades.transformaciones import to_decimal_time, to_fecha_hora

def calculo_viajes_en_vacio(
    dir_datos_viajes: str,
    dir_datos_velocidades: str,
    dir_datos_icf: str,
    dir_datos_patentes: str,
    dir_diccionario: str,
    dir_periodos: str,
    dir_exclusion_periodos: str,
    dir_exclusion_servicios: str,
    dir_resultados: str = "resultados_vv",
    unir_datos: bool = True,
    check: bool = True,
):
    """Calcula las expediciones a excluirse del cálculo de los indicadores.

    :param dir_datos_viajes: Ruta de la carpeta con datos de los viajes sin transacciones
    :type dir_datos_viajes: str

    :param dir_datos_velocidades: Ruta de la carpeta con info de las velocidades de operación
    :type dir_datos_velocidades: str

    :param dir_datos_icf: Ruta de la carpeta con datos de ICF
    :type dir_datos_icf: str

    :param dir_datos_patentes: Ruta de la carpeta con info de las patentes
    :type dir_datos_patentes: str

    :param dir_diccionario: Ruta de la del archivo con diccionario de codigos
    :type dir_diccionario: str

    :param dir_periodos: Ruta del archivo con los info de los periodos
    :type dir_periodos: str

    :param dir_exclusion_periodos: Ruta del archivo con los periodos excluidos
    :type dir_exclusion_periodos: str

    :param dir_exclusion_servicios: Ruta del archivo con los servicios excluidos
    :type dir_exclusion_servicios: str

    :param dir_resultados: Ruta carpeta donde se guardan los resultados
    :type dir_resultados: str [opcional]

    :param unir_datos: Si se deben concatenar los archivos de datos en las carpetas (True o False)
    :type unir_datos: bool [opcional]

    :param check: Si es True se chequea si para algún servicio no se encontraron datos en los archivos del ICF.
    :type check: bool [opcional]

    :return: 4 archivos
    1. plazas y distancias por unidad de negocio en 1era quincena ubicado en "resultados/1er_quincena_ICT.xlsx"
    2. plazas y distancias por unidad de negocio en 2da quincena ubicado en "resultados/2da_quincena_ICT.xlsx"
    3. Resumen de viajes sin transacciones ubicadon en "resultados/viaje_sin_trx_resumen.xlsx"
    4. Tabla de viajes a eliminar ubicado en "resultados/viajes_eliminar_ICF_ICR.xlsx"

    :Example:
    calculos.calculo_calculo_viajes_en_vacio(
        dir_datos_viajes='datos_viajes_sin_transacciones',
        dir_datos_velocidades='datos_velocidades_servicio',
        dir_datos_icf='datos_icf',
        dir_datos_patentes='datos_patentes',
        dir_diccionario='Diccionario_Servicios.xlsx',
        dir_periodos='Periodos.xlsx',
        dir_exclusion_periodos='Exclusion_Periodos.xlsx',
        dir_exclusion_servicios='Exclusion_Servicios.xlsx'
    )

    """

    tiempo_inicio = time.time()
    nombre_consolidado_viajes = "viajes_sin_transacciones_mes.csv"
    nombre_consolidado_velocidades = "velocidades_mes.csv"
    nombre_consolidado_icf = "icf_mes.csv"
    nombre_consolidado_patentes = "patentes_mes.csv"

    print("\n              # -- # -- # Cálculo de Viajes en Vacio # -- # -- #      \n")
    print("\n   [1/4] Cargando/Concatenando archivos \n")
    if unir_datos:
        concatenar_desde_carpeta(dir_datos_viajes, nombre_consolidado_viajes)

        concatenar_desde_carpeta(
            dir_datos_velocidades, nombre_consolidado_velocidades, "excel", "velocidad"
        )

        concatenar_desde_carpeta(dir_datos_icf, nombre_consolidado_icf, "excel", "SSPD")

        concatenar_desde_carpeta(
            dir_datos_patentes, nombre_consolidado_patentes, tipo="excel", encabezado=3
        )

    df_viajes = abrir_archivo(
        nombre_consolidado_viajes,
        "csv",
        dir_datos_viajes,
        "Archivo consolidado de viajes no se encuentra",
    )

    df_lbs = abrir_archivo(
        nombre_consolidado_velocidades,
        "csv",
        dir_datos_velocidades,
        "Archivo consolidado de velocidades no se encuentra",
    )

    df_po = abrir_archivo(
        nombre_consolidado_icf,
        "csv",
        dir_datos_icf,
        "Archivo consolidado de PO no se encuentra",
    )

    df_plazas = abrir_archivo(
        nombre_consolidado_patentes,
        "csv",
        dir_datos_patentes,
        "Archivo consolidado de plazas no se encuentra",
    )

    df_dic = abrir_archivo(
        dir_diccionario, "xlsx", mensaje_error="Archivo de diccionario no se encuentra"
    )
    df_periodos = abrir_archivo(
        dir_periodos, "xlsx", mensaje_error="Archivo de periodos no se encuentra"
    )
    df_ex_servicios = abrir_archivo(
        dir_exclusion_servicios,
        "xlsx",
        mensaje_error="Archivo de exclusion de servicios no se encuentra",
    )
    df_ex_periodos = abrir_archivo(
        dir_exclusion_periodos,
        "xlsx",
        mensaje_error="Archivo de exclusion de periodos no se encuentra",
    )

    print("   [2/4] Formateando datos \n")

    df_viajes["Fecha"] = pd.to_datetime(df_viajes["Fecha"], format="%d/%m/%Y")
    df_viajes["Sentido"] = df_viajes["Sentido"].str.upper()
    df_viajes["Servicio"] = df_viajes["Servicio"].str.upper()

    año = df_viajes["Fecha"][0].year
    mes = df_viajes["Fecha"][0].month

    df_lbs["Sentido"] = df_lbs["Sentido"].str.upper()
    df_lbs["Servicio"] = df_lbs["Servicio"].str.upper()
    df_lbs["Fecha"] = pd.to_datetime(df_lbs["Fecha"], format="%d/%m/%Y")
    
    df_lbs["Inicio"] = pd.to_datetime(df_lbs["Inicio"], format="%H:%M:%S")
    df_lbs["Fin"] = pd.to_datetime(df_lbs["Fin"], format="%H:%M:%S")

    df_po["Servicio"] = df_po["Servicio"].str.upper()
    df_po["Fecha"] = pd.to_datetime(df_po["Fecha"], format="%d/%m/%Y")

    df_plazas = df_plazas.drop_duplicates("Patente")

    df_dic["Sentido"] = df_dic["Sentido"].str.upper()
    df_dic["Codigo_Sinoptico"] = df_dic["Codigo_Sinoptico"].str.upper()
    df_dic["Codigo_PO"] = df_dic["Codigo_PO"].str.upper()
    df_dic["Codigo_SIG"] = df_dic["Codigo_SIG"].str.upper()
    df_dic["Codigo_Usuario"] = df_dic["Codigo_Usuario"].str.upper()

    df_ex_servicios["Sentido"] = df_ex_servicios["Sentido"].str.upper()
    df_ex_servicios["Servicio"] = df_ex_servicios["Servicio"].str.upper()

    print("   [3/4] Generando nuevas tablas \n")
    # -- # -- # JOIN tabla viajes y tabla diccionario (para periodos "Codigo_PO", "Vigencia", "Codigo_SIG")--> tabla final # -- # -- #
    df_final = pd.merge(
        df_viajes,
        df_dic[["Codigo_Sinoptico", "Sentido", "Codigo_PO", "Vigencia", "Codigo_SIG"]],
        left_on=["Servicio", "Sentido"],
        right_on=["Codigo_Sinoptico", "Sentido"],
        how="left",
        suffixes=(None, "_y"),
    )

    # -- # -- # JOIN tabla final y tabla periodos (para Cod_SIG) --> tabla final # -- # -- #
    df_final = pd.merge(
        df_final,
        df_periodos[["Cod_Sinptico", "Cod_SIG"]],
        left_on="Periodo",
        right_on="Cod_Sinptico",
        how="left",
        suffixes=(None, "_y"),
    ).rename(columns={"Cod_SIG": "Periodo_SIG"})

    # -- # -- # JOIN tabla final y tabla exclusion de servicios (para servicios excluidos) --> tabla final # -- # -- #
    df_final = pd.merge(
        df_final,
        df_ex_servicios[["Sentido", "Servicio", "Periodo", "Motivo"]],
        left_on=["Sentido", "Servicio", "Periodo"],
        right_on=["Sentido", "Servicio", "Periodo"],
        how="left",
        suffixes=(None, "_y"),
    ).rename(columns={"Motivo": "Exclusion_Servicio"})

    # -- # -- # JOIN tabla final y tabla exclusion de periodos (para periodos excluidos) --> tabla final # -- # -- #
    df_final = pd.merge(
        df_final,
        df_ex_periodos[["Periodo", "Exclusión"]],
        on="Periodo",
        how="left",
        suffixes=(None, "_y"),
    ).rename(columns={"Exclusión": "Exclusion_Periodos"})
    # print(df_final.loc[df_final['Servicio']=='433',['Servicio', 'Codigo_PO']].head())
    # -- # -- # JOIN tabla final y tabla ICF (para Observacion SSPD y Distancia PO) --> tabla final # -- # -- #
    df_final = pd.merge(
        df_final,
        df_po[
            [
                "Periodo",
                "Sentido",
                "Servicio",
                "Fecha",
                "Observacion SSPD",
                "Distancia PO",
            ]
        ],
        left_on=["Periodo_SIG", "Sentido", "Codigo_PO", "Fecha"],
        right_on=["Periodo", "Sentido", "Servicio", "Fecha"],
        how="left",
        suffixes=(None, "_y"),
    ).rename(columns={"Observacion SSPD": "Exclusion_ICF_ICR"})

    # -- # -- # JOIN tabla final y tabla plazas patentes --> tabla final (para Plazas)# -- # -- #
    df_final = pd.merge(
        df_final,
        df_plazas[["Patente", "Plazas"]],
        left_on="PPU",
        right_on="Patente",
        how="left",
        suffixes=(None, "_y"),
    )

    df_final["Vigencia"] = df_final["Vigencia"].str.upper()

    # -- # -- # Filtrar # -- # -- #
    df_final = df_final.loc[
        (df_final["Exclusion_Servicio"].isna())
        & (df_final["Exclusion_ICF_ICR"].isna())
        & (df_final["Exclusion_Periodos"].isna())
        & (df_final["Vigencia"] == "OK"),
        [
            "Fecha",
            "Unidad de Negocio",
            "Servicio",
            "Periodo",
            "PPU",
            "Sentido",
            "Hora Inicio Viaje",
            "Hora Fin de Viaje",
            "Total Transacciones",
            "Velocidad del Viaje (PPU)",
            "Velocidad Media (SSPD)",
            "Transacciones Medias (SSPD)",
            "Codigo_PO",
            "Vigencia",
            "Periodo_SIG",
            "Codigo_SIG",
            "Exclusion_Servicio",
            "Exclusion_Periodos",
            "Exclusion_ICF_ICR",
            "Plazas",
            "Distancia PO",
        ],
    ]
    df_final["Hora Inicio Viaje"] = pd.to_datetime(df_final["Hora Inicio Viaje"], format="%H:%M:%S")
    df_final["Hora Fin de Viaje"] = pd.to_datetime(df_final["Hora Fin de Viaje"], format="%H:%M:%S")

    # -- # -- # Agregacion para distancia y plazas por unidad de negocio en 1era quincena # -- # -- #
    df_1er_quincena = df_final.loc[
        df_final["Fecha"] <= datetime.datetime(año, mes, 15),
        ["Unidad de Negocio", "Plazas", "Distancia PO"],
    ]
    df_1er_quincena["Plazas_KM"] = (
        df_1er_quincena["Plazas"] * df_1er_quincena["Distancia PO"]
    )
    df_1er_quincena_gb = df_1er_quincena.groupby("Unidad de Negocio").sum()[
        ["Plazas_KM"]
    ]

    # -- # -- # Agregacion para distancia y plazas por unidad de negocio en 2da quincena # -- # -- #
    df_2da_quincena = df_final.loc[
        df_final["Fecha"] > datetime.datetime(año, mes, 15),
        ["Unidad de Negocio", "Plazas", "Distancia PO"],
    ]
    df_2da_quincena["Plazas_KM"] = (
        df_2da_quincena["Plazas"] * df_2da_quincena["Distancia PO"]
    )
    df_2da_quincena_gb = df_2da_quincena.groupby("Unidad de Negocio").sum()[
        ["Plazas_KM"]
    ]

    # -- # -- # Determinar tabla viajes a eliminar # -- # -- #
    viajes_a_eliminar_de_ICF_ICR = pd.merge(
        df_final,
        df_lbs[["Servicio", "Sentido", "Fecha", "Patente", "Inicio", "Fin"]],
        how="left",
        left_on=["Codigo_SIG", "Sentido", "Fecha", "PPU"],
        right_on=["Servicio", "Sentido", "Fecha", "Patente"],
        suffixes=(None, "_y"),
    )

    viajes_a_eliminar_de_ICF_ICR["Inicio +9"] = viajes_a_eliminar_de_ICF_ICR[
        "Inicio"
    ] + pd.Timedelta(minutes=9)

    viajes_a_eliminar_de_ICF_ICR["Inicio -9"] = viajes_a_eliminar_de_ICF_ICR[
        "Inicio"
    ] - pd.Timedelta(minutes=9)

    viajes_a_eliminar_de_ICF_ICR = viajes_a_eliminar_de_ICF_ICR.loc[
        (
            viajes_a_eliminar_de_ICF_ICR["Hora Inicio Viaje"].dt.time
            >= viajes_a_eliminar_de_ICF_ICR["Inicio -9"].dt.time
        )
        & (
            viajes_a_eliminar_de_ICF_ICR["Hora Inicio Viaje"].dt.time
            <= viajes_a_eliminar_de_ICF_ICR["Inicio +9"].dt.time
        ),
        [
            "Fecha",
            "Unidad de Negocio",
            "Servicio",
            "Periodo",
            "PPU",
            "Sentido",
            "Hora Inicio Viaje",
            "Hora Fin de Viaje",
            "Total Transacciones",
            "Velocidad del Viaje (PPU)",
            "Velocidad Media (SSPD)",
            "Transacciones Medias (SSPD)",
            "Codigo_PO",
            "Vigencia",
            "Periodo_SIG",
            "Codigo_SIG",
            "Exclusion_Servicio",
            "Exclusion_Periodos",
            "Exclusion_ICF_ICR",
            "Plazas",
            "Distancia PO",
            "Inicio",
            "Fin",
        ],
    ].rename(columns={"Inicio": "Hora_Inicio_SIG", "Fin": "Hora_Fin_SIG"})


    # -- # -- # Modificar tipo de datos en columnas # -- # -- #
    df_final["Hora Inicio Viaje"] = df_final["Hora Inicio Viaje"].apply(to_decimal_time)
    df_final["Hora Fin de Viaje"] = df_final["Hora Fin de Viaje"].apply(to_decimal_time)
    df_final["Fecha"] = df_final["Fecha"].dt.date

    viajes_a_eliminar_de_ICF_ICR = viajes_a_eliminar_de_ICF_ICR.drop_duplicates()
    viajes_a_eliminar_de_ICF_ICR["Unidad de Negocio"] = viajes_a_eliminar_de_ICF_ICR[
        "Unidad de Negocio"
    ].str.slice(0, 2)
    viajes_a_eliminar_de_ICF_ICR["Periodo"] = (
        viajes_a_eliminar_de_ICF_ICR["Periodo_SIG"].str.slice(0, 3).astype(int)
    )
    viajes_a_eliminar_de_ICF_ICR["Inicio Sig"] = 0
    viajes_a_eliminar_de_ICF_ICR = viajes_a_eliminar_de_ICF_ICR.apply(
        to_fecha_hora, axis=1
    )
    # viajes_a_eliminar_de_ICF_ICR = viajes_a_eliminar_de_ICF_ICR[
    #     ["Unidad de Negocio", "PPU", "Periodo", "Hora Inicio Viaje","Inicio Sig"]
    # ]

    print("   [4/4] Guardando datos \n")
    # -- # -- # Guardar tablas # -- # -- #
    exist = os.path.exists(dir_resultados)
    if not exist:
        os.makedirs(dir_resultados)

    if check:
        if df_final['Distancia PO'].isnull().values.any():
            servicios_null = list(df_final.loc[df_final['Distancia PO'].isna(),'Servicio'].unique())
            raise ValueError(f'Hay valores nulos en la columna "Distancia PO" para los servicios: {servicios_null}. No se encontraron viajes en el archivo de ICF para los viajes sin transacciones.')
    
    df_final.to_excel(
        os.path.join(dir_resultados, "viaje_sin_trx_resumen.xlsx"), index=False
    )
    df_1er_quincena_gb.to_excel(os.path.join(dir_resultados, "1er_quincena_ICT.xlsx"))
    df_2da_quincena_gb.to_excel(os.path.join(dir_resultados, "2da_quincena_ICT.xlsx"))
    viajes_a_eliminar_de_ICF_ICR.to_excel(
        os.path.join(dir_resultados, "viajes_eliminar_ICF_ICR.xlsx"), index=False
    )
    tiempo_final = round((time.time() - tiempo_inicio) / 60, 3)
    print("\n              # -- # -- # Termino # -- # -- #      \n")
    print(f"    Tiempo total: {tiempo_final} minutos\n")