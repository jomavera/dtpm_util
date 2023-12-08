import datetime
import os
from typing import List

import pandas as pd
from dtpm_util.utilidades.excel.hoja_calculo import leer_hoja


def concatenar_anexo_8(dir_carpeta_archivos, dir_carpeta_resultados):
    """Concatena anexos 8 que se encuentra en "dir_carpeta_archivos".

    :param dir_carpeta_archivos: Ruta de la carpeta con carpetas de anexos 4 y 8.
    :type dir_carpeta_archivos: str

    :param dir_carpeta_resultados: Ruta de la carpeta donde se almacenará el resultado.
    :type dir_carpeta_resultados: str

    :return: Archivo ubicado en "~/[dir_carpeta_resultados]/anexo_8_merge.xls" con los anexos 4 y anexos 8 concatenados.
    """

    nombre_archivo_salida = "anexo_8_merge.xlsx"
    dir_anexos_4 = os.path.join(dir_carpeta_archivos, "anexo_4")
    dir_anexos_8 = os.path.join(dir_carpeta_archivos, "anexo_8")

    df = pd.DataFrame(
        columns=["id_servicio", "sentido", "tipo_dia", "hora", "id_tipo_bus"]
    )

    # -- # -- # concatenar anexos 4 # -- # -- #
    print("  ------ Concatenacion de anexos 4 \n")
    uns = os.listdir(dir_anexos_4)
    dfs = []
    for un in uns:
        archivos = os.listdir(os.path.join(dir_anexos_4, un))
        for archivo in archivos:
            dir_archivo = os.path.join(dir_anexos_4, un, archivo)

            print(f"  Concatenando archivo: {dir_archivo} ")

            df_temp = pd.read_excel(
                dir_archivo,
                header=6,
                usecols=[
                    "CODIGO TS SERVICIO",
                    "SENTIDO",
                    "TIPO_DIA",
                    "HORA_INICIO",
                    "TIPO_BUS",
                    "TIPO_EVENTO",
                ],
                sheet_name="Tabla Horaria",
                dtype={"id_servicio": str},
            ).rename(
                columns={
                    "CODIGO TS SERVICIO": "id_servicio",
                    "SENTIDO": "sentido",
                    "TIPO_DIA": "tipo_dia",
                    "HORA_INICIO": "hora",
                    "TIPO_BUS": "id_tipo_bus",
                }
            )

            df_temp = df_temp.loc[
                :,
                ["id_servicio", "sentido", "tipo_dia", "hora", "id_tipo_bus"],
            ]
            fecha = archivo[:2] + "/" + archivo[2:4] + "/" + archivo[4:-5]
            df_temp["fecha_po"] = fecha
            dfs.append(df_temp)
    if len(dfs) == 0:
        df = pd.DataFrame()
    else:
        df = pd.concat(dfs)

    # -- # -- # concatenar anexos 8 # -- # -- #
    print("\n  ------ Concatenacion de anexos 8 \n")
    uns = os.listdir(dir_anexos_8)
    for un in uns:
        archivos = os.listdir(os.path.join(dir_anexos_8, un))
        for ix, archivo in enumerate(archivos):
            dir_archivo = os.path.join(dir_anexos_8, un, archivo)

            print(f"  Concatenando archivo: {dir_archivo} ")

            xlsx = pd.ExcelFile(dir_archivo)
            hojas = [
                hoja
                for hoja in xlsx.sheet_names
                if hoja != "Capacidades" and hoja != "Capacidad"
            ]
            fecha = archivo[:2] + "/" + archivo[2:4] + "/" + archivo[4:-5]
            for hoja in hojas:
                df = leer_hoja(df, xlsx, hoja, "B:C", "Laboral", "Ida", fecha_po=fecha)
                df = leer_hoja(df, xlsx, hoja, "D:E", "Laboral", "Ret", fecha_po=fecha)
                df = leer_hoja(df, xlsx, hoja, "F:G", "Laboral", "Ida", fecha_po=fecha)
                df = leer_hoja(df, xlsx, hoja, "H:I", "Laboral", "Ret", fecha_po=fecha)
                df = leer_hoja(df, xlsx, hoja, "J:K", "Sábado", "Ida", fecha_po=fecha)
                df = leer_hoja(df, xlsx, hoja, "L:M", "Sábado", "Ret", fecha_po=fecha)
                df = leer_hoja(df, xlsx, hoja, "N:O", "Sábado", "Ida", fecha_po=fecha)
                df = leer_hoja(df, xlsx, hoja, "P:Q", "Sábado", "Ret", fecha_po=fecha)
                df = leer_hoja(df, xlsx, hoja, "R:S", "Domingo", "Ida", fecha_po=fecha)
                df = leer_hoja(df, xlsx, hoja, "T:U", "Domingo", "Ret", fecha_po=fecha)
                df = leer_hoja(df, xlsx, hoja, "V:W", "Domingo", "Ida", fecha_po=fecha)
                df = leer_hoja(df, xlsx, hoja, "X:Y", "Domingo", "Ret", fecha_po=fecha)

    df.dropna(inplace=True)
    if os.path.exists(os.path.join(dir_carpeta_resultados, nombre_archivo_salida)):
        os.remove(os.path.join(dir_carpeta_resultados, nombre_archivo_salida))

    df = df.loc[
        :, ["id_servicio", "sentido", "tipo_dia", "hora", "id_tipo_bus", "fecha_po"]
    ]

    df["hora"] = df["hora"].apply(
        lambda x: x.time() if isinstance(x, datetime.datetime) else x
    )

    df.to_excel(
        os.path.join(dir_carpeta_resultados, nombre_archivo_salida), index=False
    )


def calcular_po(
    dir_anexo_8: str, dir_dicc_buses: str, dir_periodos: str, dir_resultado: str
):
    """Calcula el po.

    :param dir_anexo_8: Ruta del archivo anexo 8.
    :type dir_anexo_8: str

    :param dir_dicc_buses: Ruta del archivo diccionario de buses.
    :type dir_dicc_buses: str

    :param dir_periodos: Ruta del archivo diccionario de periodos.
    :type dir_periodos: str

    :param dir_resultado: Ruta de la carpeta donde se almacenará el resultado.
    :type dir_resultado: str

    :return: Archivo ubicado en "~/[dir_resultado]/datos_po.xlsx" con el po.
    """

    df = pd.read_excel(dir_anexo_8, dtype={"hora": str})

    df["hora"] = df["hora"].str.slice(0, 8)
    df["hora"] = pd.to_datetime(df["hora"], format="%H:%M:%S")
    df["hora"] = df["hora"].dt.floor("30min")

    df_periodos = pd.read_excel(dir_periodos)
    df_periodos["Media Hora"] = pd.to_datetime(
        df_periodos["Media Hora"], format="%H:%M:%S"
    )

    df_tipo_bus = pd.read_excel(dir_dicc_buses)

    df = df.merge(df_periodos, left_on="hora", right_on="Media Hora", how="left")
    df = df.merge(df_tipo_bus, left_on="id_tipo_bus", right_on="id_tipo_bus")
    df = (
        df[["id_servicio", "fecha_po", "Nombre2", "sentido", "tipo_dia", "plazas"]]
        .groupby(
            ["id_servicio", "fecha_po", "Nombre2", "sentido", "tipo_dia"],
            as_index=False,
        )
        .sum()
    )

    df.to_excel(os.path.join(dir_resultado, "datos_po.xlsx"), index=False)


def calcular_consolidado_lbs_patentes(
    dir_lbs: str, dir_patentes: str, dir_resultado: str
):
    """
    Calcula el consolidado de lbs y patentes.

    :param dir_lbs: Ruta de la carpeta con los archivos lbs.
    :type dir_lbs: str

    :param dir_patentes: Ruta de la carpeta con los archivos de patentes.
    :type dir_patentes: str

    :param dir_resultado: Ruta de la carpeta donde se almacenará el resultado.
    :type dir_resultado: str

    :return: Archivo ubicado en "~/[dir_resultado]/datos_observados.csv" con el consolidado de lbs y patentes.
    """

    print("  ------ Calculando consolidado lbs y patentes \n")
    archivos_lbs = os.listdir(dir_lbs)
    archivos_plazas = os.listdir(dir_patentes)

    dfs_lbs = []
    for archivo_lbs in archivos_lbs:
        dfs_lbs.append(
            pd.read_excel(
                os.path.join(dir_lbs, archivo_lbs),
                sheet_name="velocidad",
                usecols=[
                    "Patente",
                    "Servicio",
                    "Sentido",
                    "Fecha",
                    "Período",
                ],
            )
        )

    df_lbs = pd.concat(dfs_lbs)

    dfs_plazas = []
    for archivo_plazas in archivos_plazas:
        df_t = pd.read_excel(
            os.path.join(dir_patentes, archivo_plazas), usecols=["PLACA", "PLAZAS"]
        )
        df_t["PLACA"] = df_t["PLACA"].apply(lambda x: x[:-2] + "-" + x[-2:])
        dfs_plazas.append(df_t)

    df_plazas = pd.concat(dfs_plazas)
    df_plazas.drop_duplicates(inplace=True)

    df = df_lbs.merge(df_plazas, left_on="Patente", right_on="PLACA", how="left")
    df.to_csv(os.path.join(dir_resultado, "datos_observados.csv"), index=False)


def calcular_plazas(
    servicios: List[str],
    mapa_po: dict,
    dir_lbs: str,
    dir_patentes: str,
    dir_anexos: str,
    dir_dicc_buses: str,
    dir_dicc_periodos: str,
    dias_especiales: dict = None,
    dir_resultados: str = "resultados",
    unir_datos: bool = True,
):
    
    """Calcula las plazas.

    :param servicios: Lista de servicios.
    :type servicios: List[str]

    :param mapa_po: Diccionario con el mapeo de servicios.
    :type mapa_po: dict

    :param dir_lbs: Ruta de la carpeta con los archivos lbs.
    :type dir_lbs: str

    :param dir_patentes: Ruta de la carpeta con los archivos de patentes.
    :type dir_patentes: str

    :param dir_anexos: Ruta de la carpeta con carpetas de anexos 4 y 8.
    :type dir_anexos: str

    :param dir_dicc_buses: Ruta del archivo diccionario de buses.
    :type dir_dicc_buses: str

    :param dir_dicc_periodos: Ruta del archivo diccionario de periodos.
    :type dir_dicc_periodos: str

    :param dias_especiales: Diccionario con los días especiales.
    :type dias_especiales: dict

    :param dir_resultados: Ruta de la carpeta donde se almacenará el resultado.
    :type dir_resultados: str

    :param unir_datos: Indica si se unen los datos de lbs y patentes, y anexos del PO.
    :type unir_datos: bool

    :return: Archivo ubicado en "~/[dir_resultados]/resultado_plazas.xlsx" con las plazas.
    """

    if not os.path.exists(dir_resultados):
        os.makedirs(dir_resultados)

    map_dia = {
        0: "Laboral",
        1: "Laboral",
        2: "Laboral",
        3: "Laboral",
        4: "Laboral",
        5: "Sábado",
        6: "Domingo",
    }

    if unir_datos:
        calcular_consolidado_lbs_patentes(
            dir_lbs=dir_lbs,
            dir_patentes=dir_patentes,
            dir_resultado=dir_resultados,
        )

        concatenar_anexo_8(
            dir_carpeta_archivos=dir_anexos, dir_carpeta_resultados=dir_resultados
        )

    calcular_po(
        dir_anexo_8=os.path.join(dir_resultados, "anexo_8_merge.xlsx"),
        dir_dicc_buses=dir_dicc_buses,
        dir_periodos=dir_dicc_periodos,
        dir_resultado=dir_resultados,
    )

    df = pd.read_csv(
        os.path.join(dir_resultados, "datos_observados.csv"),
        usecols=[
            "Servicio",
            "Sentido",
            "Fecha",
            "Período",
            "PLAZAS",
        ],
    )

    df = df.loc[df["Servicio"].isin(servicios), :]

    df = df.groupby(["Servicio", "Sentido", "Período", "Fecha"], as_index=False).sum()

    df["fecha_po"] = df["Fecha"].map(mapa_po)
    df["Fecha"] = pd.to_datetime(df["Fecha"], format="%d/%m/%Y")
    df["tipo dia"] = df["Fecha"].dt.dayofweek
    df["tipo dia"] = df["tipo dia"].map(map_dia)

    if dias_especiales is not None:
        for dia, tipo in dias_especiales.items():
            df.loc[df["Fecha"] == dia, "tipo dia"] = tipo

    df_po = pd.read_excel(
        os.path.join(dir_resultados, "daotos_po.xlsx"), date_format="%d/%m/%Y"
    )
    df_po["sentido"] = df_po["sentido"].map({"Ret": "Regreso", "Ida": "Ida"})

    df = df.merge(
        df_po,
        left_on=["Servicio", "fecha_po", "Sentido", "Período", "tipo dia"],
        right_on=["id_servicio", "fecha_po", "sentido", "Nombre2", "tipo_dia"],
        how="left",
    )

    df.rename(
        columns={
            "PLAZAS": "Plazas Observadas",
            "plazas": "Plazas PO",
        },
        inplace=True,
    )

    df = df[
        [
            "Servicio",
            "Sentido",
            "Período",
            "Fecha",
            "tipo dia",
            "fecha_po",
            "Plazas Observadas",
            "Plazas PO",
        ]
    ]

    df.to_excel(os.path.join(dir_resultados, "resultado_plazas.xlsx"), index=False)
