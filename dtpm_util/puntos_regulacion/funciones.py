import os
import re
from datetime import datetime, time, timedelta
from typing import List, Tuple

import numpy as np
import pandas as pd
from shapely.geometry import Point, Polygon


def extractor_pr(
    registros_gps: pd.DataFrame,
    poligono_PR: List[Tuple[float, float]],
    id_PR: int,
    tiempo_regulando:str
):
    """Función que extrae los tiempos de regulación en un punto de regulación

    :param registros_gps: Dataframe con las pulsaciones de los buses
    :type registros_gps: pd.DataFrame

    :param poligono_PR: Lista con coordenadas que definen un poligono
    :type poligono_PR: List[Tuple[float, float]]

    :param id_PR: Identificador del punto de regulación
    :type id_PR: int

    :return: Dataframe con los tiempos de regulación de un punto de regulación
    :rtype: pandas.core.frame.DataFrame
    """
    registros_gps_cp = registros_gps.copy()
    # Filtrar primero las pulsaciones que se encuentren en el cuadrado envolvente al poligono
    latitud_pr_min, longitud_pr_min, latitud_pr_max, longitud_pr_max = poligono_PR.bounds
    condicion_prefiltro = (
        (registros_gps["Latitud"] <= latitud_pr_max + 0.005)
        & (registros_gps["Latitud"] >= latitud_pr_min - 0.005)
        & (registros_gps["Longitud"] <= longitud_pr_max + 0.005)
        & (registros_gps["Longitud"] >= longitud_pr_min - 0.005)
    )
    # condicion_prefiltro = poligono_PR.buffer(distance=100).contains(registros_gps_cp["coord"].values)
    registros_gps_cp = registros_gps_cp.loc[condicion_prefiltro, :]

    # Determinar que pulsaciones se encuentran en el poligono
    condicion = poligono_PR.contains(registros_gps_cp["coord"].values)
    
    registros_gps_cp.loc[:,'Criterio_Interior_Poligono'] = condicion

    registros_gps_cp.loc[:,"Tiempogps"] = pd.to_datetime(
        registros_gps_cp["Tiempogps"], format="%Y%m%d%H%M%S"
    )

    # Se hace un registro de patentes para determinar su regulación
    patentes_registradas = registros_gps_cp["Patente"].drop_duplicates()

    # Lista para guardar los tiempos de cada entrada
    tiempos_radio_PR = []

    # Se itera para cada patente, obteniendo los horarios de entrada y de salida del Punto de Regulación cada vez que están allí
    for placa in patentes_registradas:
        # Se obtiene un dataframe por cada patente de la iteración
        entradas_bus = registros_gps_cp[registros_gps_cp["Patente"] == placa]
        
        # Ordena las entradas por fecha y hora
        entradas_bus = entradas_bus.sort_values(by=["Tiempogps"])
        if entradas_bus.loc[entradas_bus['Criterio_Interior_Poligono'],:].shape[0] > 0:
            # Ciclo while sobre las entradas para crear los registros de regulación por patente y por su ocasión de regulación
            i = 0

            while i < len(entradas_bus):
                entrada = entradas_bus.iloc[i]
                if entrada["Criterio_Interior_Poligono"]:
                    fecha_inicio = entrada["Tiempogps"].to_pydatetime()
                    fecha_inicio_original = entrada["Tiempogps"]
                    hora_entrada = fecha_inicio.strftime("%H:%M:%S")
                    fecha_entrada = fecha_inicio.strftime("%d-%m-%Y")
                    ss_entrada = entrada["Ruta"]

                    j = i + 1

                    while (
                        j < len(entradas_bus) 
                        and entradas_bus.iloc[j]["Criterio_Interior_Poligono"]
                    ):
                        j += 1

                    fecha_fin = entradas_bus.iloc[j - 1]["Tiempogps"].to_pydatetime()
                    fecha_fin_original = entradas_bus.iloc[j - 1]["Tiempogps"]
                    hora_salida = fecha_fin.strftime("%H:%M:%S")
                    fecha_salida = fecha_fin.strftime("%d-%m-%Y")
                    ss_salida = entradas_bus.iloc[j - 1]["Ruta"]
                    tiempo_radio_delta = fecha_fin_original - fecha_inicio_original
                    semilla_segundos_t_r_d = tiempo_radio_delta.total_seconds()
                    hora_tiempo_radio = int(semilla_segundos_t_r_d // 3600)
                    minutos_tiempo_radio = int((semilla_segundos_t_r_d % 3600) // 60)
                    segundos_tiempo_radio = int(semilla_segundos_t_r_d % 60)
                    auxiliar_tiempo_radio = time(
                        hora_tiempo_radio, minutos_tiempo_radio, segundos_tiempo_radio
                    )
                    tiempo_radio = auxiliar_tiempo_radio.strftime("%H:%M:%S")
                    tiempos_radio_PR.append(
                        [
                            placa,
                            fecha_inicio_original,
                            fecha_entrada,
                            hora_entrada,
                            ss_entrada,
                            fecha_fin_original,
                            fecha_salida,
                            hora_salida,
                            ss_salida,
                            tiempo_radio,
                        ]
                    )
                    i = j
                else:
                    i += 1

    # Se crea el data frame de salida de las patentes y servicios asociados que están regulando en el PR
    df_anteprevio_salida = pd.DataFrame(
        tiempos_radio_PR,
        columns=[
            "Patente",
            "Ingreso Datetime",
            "Fecha Entrada PR",
            "Hora Entrada PR",
            "SS Entrada PR",
            "Egreso Datetime",
            "Fecha Salida PR",
            "Hora Salida PR",
            "SS Salida PR",
            "Tiempo en el PR",
        ],
    )

    # Borra los registros cuya permanencia en el PR es menor a cinco minutos
    df_previo_salida = df_anteprevio_salida[
        df_anteprevio_salida["Tiempo en el PR"] >= tiempo_regulando
    ].reset_index(drop=True)

    # Se agregan las columnas de latitud y longitud del data frame df_salida por el lado izquierdo
    df_previo_salida["ID PR"] = id_PR

    return df_previo_salida


def carga_patentes_seremitt(carpeta: str, nom_registros_patentes: str):
    """Función que carga los archivos de patentes de la SEREMITT RM

    param: carpeta: Ruta donde se encuentran los archivos de patentes de la SEREMITT RM
    type: carpeta: str

    param: nom_registros_patentes: Nombre tipo de los archivos de patentes de la SEREMITT RM
    type: nom_registros_patentes: str

    :return: Dataframe con los registros de patentes de la SEREMITT RM
    :rtype: pandas.core.frame.DataFrame
    """

    patentes_seremitt = [
        x
        for x in os.listdir(carpeta)
        if re.search(nom_registros_patentes, x)
        and os.path.isfile(os.path.join(carpeta, x))
    ]
    lista_patentes_seremitt = []

    for ix, archivo_seremitt in enumerate(patentes_seremitt):
        print(
            "Procesando archivo de patentes SEREMITT RM "
            + str(ix + 1)
            + " de "
            + str(len(patentes_seremitt))
        )
        data_ppu_seremitt = pd.read_excel(
            os.path.join(carpeta, archivo_seremitt), engine="openpyxl"
        )

        aux_01 = archivo_seremitt.replace("Consolidado_RED_", "").replace(".xlsx", "")
        aux_02_fecha_ppu_seremitt = aux_01[0:2] + "-" + aux_01[2:4] + "-" + aux_01[4:]
        data_ppu_seremitt["Fecha"] = aux_02_fecha_ppu_seremitt
        lista_patentes_seremitt.append(data_ppu_seremitt)

    PPU_seremitt = pd.concat(lista_patentes_seremitt)
    PPU_seremitt["PLACA_Fecha"] = PPU_seremitt["PLACA"] + PPU_seremitt["Fecha"]

    return PPU_seremitt


def determinar_un(df_pulsaciones: pd.DataFrame, df_patentes: pd.DataFrame):
    """Función que cruza tiempos de regulacion con patentes para determinar unidad de negocio por tiempo de regulacion

    param: df_pulsaciones: Dataframe con los registros de las pulsaciones de los buses
    type: df_pulsaciones: pandas.core.frame.DataFrame

    param: df_patentes: Dataframe con los registros de las patentes de los buses
    type: df_patentes: pandas.core.frame.DataFrame

    :return: Dataframe con los registros de las demoras de los buses en los PR
    :rtype: pandas.core.frame.DataFrame
    """

    salida_aux_01 = pd.merge(
        df_pulsaciones,
        df_patentes,
        left_on="PLACA_Fecha_Entrada",
        right_on="PLACA_Fecha",
    )
    salida_aux_01.drop(columns=["PLACA_Fecha"], inplace=True)
    salida_aux_01.rename({"UN": "UN Entrada PR"}, axis=1, inplace=True)
    salida_aux_01 = pd.merge(
        salida_aux_01, df_patentes, left_on="PLACA_Fecha_Salida", right_on="PLACA_Fecha"
    )
    bd_buses_pr_demoras = salida_aux_01[
        [
            "ID PR",
            "ID Patente",
            "Patente",
            "Ingreso Datetime",
            "Fecha Entrada PR",
            "Hora Entrada PR",
            "SS Entrada PR",
            "UN Entrada PR",
            "Egreso Datetime",
            "Fecha Salida PR",
            "Hora Salida PR",
            "SS Salida PR",
            "UN",
            "Tiempo en el PR",
        ]
    ]
    bd_buses_pr_demoras.rename({"UN": "UN Salida PR"}, axis=1, inplace=True)
    return bd_buses_pr_demoras


def procesamiento_registros_pr(
    carpeta: str,
    nom_registros_gps: str,
    listado_PR: pd.DataFrame,
    loaded_files: List[str] = [],
    tiempo_regulando: str = "00:10:00",
):
    """Función que procesa los registros de pulsaciones de los buses para determinar los tiempo de regulacion en cada PR

    param: carpeta: Ruta donde se encuentran los archivos de pulsaciones de los buses
    type: carpeta: str

    param: nom_registros_gps: Nombre tipo de los archivos de pulsaciones de los buses
    type: nom_registros_gps: str

    param: listado_PR: Dataframe con los registros de los PR
    type: listado_PR: pandas.core.frame.DataFrame

    param: loaded_files: Lista con los archivos ya procesados
    type: loaded_files: List[str]

    :return: Dataframe con los tiempos de regulacion de los buses en cada PR procesados de todos los archivos de la carpeta
    :rtype: pandas.core.frame.DataFrame
    """

    csv_pulsaciones = [
        x
        for x in os.listdir(carpeta)
        if re.search(nom_registros_gps, x) and os.path.isfile(os.path.join(carpeta, x))
    ]

    ids_PR = list(listado_PR["id_pr"].unique())

    lista_df_salida_PR = []

    print(
        "\n # -- # -- # -- # -- # Carga y Procesamiento de datos pulsaciones # -- # -- # -- # -- #\n"
    )
    for ix, archivo_pulsaciones in enumerate(csv_pulsaciones):
        print(
            "Procesando archivo de pulsaciones GPS "
            + str(ix + 1)
            + " de "
            + str(len(csv_pulsaciones))
            + "\n"
        )

        if archivo_pulsaciones.replace(".csv", "_procesado_pol.csv") not in loaded_files:
            data_pulsaciones = pd.read_csv(
                os.path.join(carpeta, archivo_pulsaciones),
                sep=";",
                encoding="latin-1",
            )
            
            # Creamos dos data frame con los datos relevantes para este análisis
            registros_gps = (
                data_pulsaciones[
                    ["Patente", "Latitud", "Longitud", "Tiempogps", "Ruta"]
                ]
                .copy()
                .reset_index(drop=True)
            )
            registros_gps = registros_gps[registros_gps['Longitud']!='78 01I']
            registros_gps['Longitud'] = registros_gps['Longitud'].astype(float)
            # Determinamos las señales gps dentro del poligonos
            registros_gps["coord"] = tuple(
                zip(registros_gps["Latitud"], registros_gps["Longitud"])
            )
            registros_gps["coord"] = registros_gps["coord"].apply(lambda x: Point(x))
            lista_PR_dia = []
            for id_ in ids_PR:
                # print("           Procesando PR " + str(i + 1) + " de " + str(size_listado_PR))
                PR = extractor_pr(
                    registros_gps,
                    Polygon(
                        [
                            x
                            for x in zip(
                                listado_PR.loc[
                                    listado_PR["id_pr"] == id_, "latitud"
                                ].values,
                                listado_PR.loc[
                                    listado_PR["id_pr"] == id_, "longitud"
                                ].values,
                            )
                        ]
                    ),
                    id_,
                    tiempo_regulando
                )
                lista_PR_dia.append(PR)

            df_salida_PR = pd.concat(lista_PR_dia)
            print("           Archivo procesado, guardando...\n")
            df_salida_PR.to_csv(
                os.path.join(
                    "checkpoints", archivo_pulsaciones.replace(".csv", "_procesado_pol.csv")
                ),
                sep=";",
                index=False,
                encoding="latin-1",
                decimal=",",
            )
            lista_df_salida_PR.append(df_salida_PR)
        else:
            print("           Archivo anteriormente procesado, cargando...\n")
            lista_df_salida_PR.append(
                pd.read_csv(
                    os.path.join(
                        "checkpoints",
                        archivo_pulsaciones.replace(".csv", "_procesado_pol.csv"),
                    ),
                    sep=";",
                    encoding="latin-1",
                )
            )

    salida_PR_preliminar = pd.concat(lista_df_salida_PR)
    # Se crean los ID de la regulación global por patente
    salida_PR_preliminar["ID Patente"] = (
        salida_PR_preliminar["Patente"].astype("category").cat.codes + 1
    )

    # Se crean los ID de la regulación individualizada dentro de la regulación de una patente, o sea,
    # para determinar cuántas regulaciones registró una patente en el periodo analizado de pulsaciones de GPS
    salida_PR_preliminar["ID Regulación Patente"] = (
        salida_PR_preliminar.groupby("ID Patente").cumcount() + 1
    )
    # Creamos la columna de patente en la salida de PR que no tenga guión '-'
    salida_PR_preliminar["PLACA"] = salida_PR_preliminar["Patente"].apply(
        lambda x: x.replace("-", "")
    )

    # Luego, concatenamos PLACA y Fechas de Entrada y Salida al PR para hacer posteriormente los merges
    salida_PR_preliminar["PLACA_Fecha_Entrada"] = (
        salida_PR_preliminar["PLACA"] + salida_PR_preliminar["Fecha Entrada PR"]
    )
    salida_PR_preliminar["PLACA_Fecha_Salida"] = (
        salida_PR_preliminar["PLACA"] + salida_PR_preliminar["Fecha Salida PR"]
    )

    return salida_PR_preliminar


def contar_buses_por_instante(
    df_pulsaciones: pd.DataFrame, listado_de_pr: pd.DataFrame, frecuencia="15min"
):
    """Función que cuenta la cantidad de buses que ingresan y egresan de regular de cada PR en cada instante de tiempo

    param: df_pulsaciones: Dataframe con los tiempos de regulacion
    type: df_pulsaciones: pandas.core.frame.DataFrame

    param: listado_de_pr: Dataframe con la informacion de los PR
    type: listado_de_pr: pandas.core.frame.DataFrame

    param: frecuencia: Frecuencia de medición de regulaciones
    type: frecuencia: str

    :return: Dataframe con el conteo de buses por PR, unidad de negocio y servicio en cada instante de tiempo
    :rtype: pandas.core.frame.DataFrame
    """

    max_fecha = max(df_pulsaciones["Fecha Entrada PR"])
    min_fecha = min(df_pulsaciones["Fecha Entrada PR"])
    instantes = pd.date_range(
        start=pd.to_datetime(min_fecha, dayfirst=True),
        end=pd.to_datetime(max_fecha, dayfirst=True) + timedelta(minutes=59, hours=23),
        freq=frecuencia,
    )

    df_instantes = pd.DataFrame(columns=["instantes"], data=instantes)
    df_pulsaciones.loc[:, "Ingreso Datetime"] = pd.to_datetime(
        df_pulsaciones["Ingreso Datetime"]
    )
    df_pulsaciones.loc[:, "Egreso Datetime"] = pd.to_datetime(
        df_pulsaciones["Egreso Datetime"]
    )

    conteos = []
    ids_pr = list(listado_de_pr["id_pr"].unique())
    for num, id_pr in enumerate(ids_pr):
        print(f"Procesando PR:{id_pr} ------- {num+1} de {len(ids_pr)} \n")
        bd_temp = df_pulsaciones[(df_pulsaciones["ID PR"] == id_pr)]
        bd_temp = bd_temp.merge(df_instantes, how="cross")
        bd_temp["conteo"] = 0
        bd_temp.loc[
            (bd_temp["Ingreso Datetime"] <= bd_temp["instantes"])
            & (bd_temp["Egreso Datetime"] >= bd_temp["instantes"]),
            "conteo",
        ] = 1

        bd_temp = (
            bd_temp[["ID PR", "UN Entrada PR", "id_servicio", "instantes", "conteo"]]
            .groupby(
                ["ID PR", "UN Entrada PR", "id_servicio", "instantes"], as_index=False
            )
            .sum()
        )
        bd_temp = bd_temp[bd_temp["conteo"] > 0]
        bd_temp["fecha"] = bd_temp["instantes"].dt.date
        conteos.append(
            bd_temp.rename(
                columns={
                    "instantes": "Instante",
                    "UN Entrada PR": "Unidad de Negocio",
                    "conteo": "Buses por instante",
                }
            )
        )

    salida_conteo_buses_un_pr = pd.concat(conteos, ignore_index=True)
    return salida_conteo_buses_un_pr

def agregar_por_un(dir_resultados):
    """Función que agrega los conteos y demoras por Unidad de Negocio

    param: dir_resultados: Ruta donde se encuentran los archivos de resultados
    type: dir_resultados: str

    :return: Dos archivos en la ruta "~/dir_resultados": "Conteos_Buses_PR_UN.csv" y "Demoras_Buses_PR_UN.csv"
    :rtype: None
    """

    df_conteo = pd.read_csv(
        os.path.join(dir_resultados, "Conteos_Buses_PR.csv"), sep=";"
    )

    df_conteo = (
        df_conteo[
            ["ID PR", "Unidad de Negocio", "Instante", "fecha", "Buses por instante"]
        ]
        .groupby(["ID PR", "Unidad de Negocio", "Instante", "fecha"], as_index=False)
        .sum()
    )

    df_conteo.to_csv(
        os.path.join(dir_resultados, "Conteos_Buses_PR_UN.csv"), index=False
    )

    df_demoras = pd.read_csv(
        os.path.join(dir_resultados, "Demoras_Buses_PR.csv"), sep=";"
    )
    df_demoras = (
        df_demoras[["ID PR", "UN Salida PR", "Ingreso Datetime", "Tiempo en el PR"]]
        .groupby(
            ["ID PR", "UN Salida PR", "Ingreso Datetime", "Tiempo en el PR"],
            as_index=False,
        )
        .sum()
    )

    df_demoras.to_csv(
        os.path.join(dir_resultados, "Demoras_Buses_PR_UN.csv"), index=False
    )