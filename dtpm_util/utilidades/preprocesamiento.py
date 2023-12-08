import datetime
import os
import re
import time

import pandas as pd
from openpyxl import Workbook
import pytz
from dtpm_util.utilidades.excel.hoja_calculo import leer_hoja, escribir_hoja_anexo_8
from dtpm_util.utilidades.transformaciones import to_cod_usuario


def concatenar_anexo_3(
    dir_carpeta_archivos: str,
    dir_carpeta_resultados: str,
    dir_subcarpeta: str = None,
    retornar: bool = False,
):
    """Concatena todos los archivos, que se encuentran en una carpeta, en un solo archivo.

    :param dir_carpeta_archivos: Ruta de carpeta con archivos de anexo 3.
    :type dir_carpeta_archivos: str

    :param dir_carpeta_resultados: Ruta de carpeta donde se guardara el archivo con los datos concatenados.
    :type dir_carpeta_resultados: str

    :param dir_subcarpeta: Ruta de subcarpeta donde se encuentran los archivos de anexo 3.
    :type dir_subcarpeta: str [opcional]

    :param retornar: Si es True retorna el dataframe con los datos concatenados.
    :type retornar: bool [opcional]

    :return: Archivo ubicado en "~/[dir_carpeta_resultados]/anexo_3_merge.xls" con los anexos 3 concatenados.
    """

    col = [
        "Unidad de Negocio",
        "Código TS",
        "Código Usuario",
        "Sentido",
        "Día",
        "MH",
        "Velocidad (Km/hra)",
        "Distancia Base (Km)",
        "Distancia Total (POB+POI) (Km)",
        "N° Salidas",
    ]

    exist = os.path.exists(dir_carpeta_resultados)
    if not exist:
        os.makedirs(dir_carpeta_resultados)

    col_2012 = [0, 1, 2, 3, 4, 6, 7, 8, 9, 10]
    col_2019 = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

    nombre_archivo_salida = "anexo_3_merge.xlsx"

    if os.path.exists(os.path.join(dir_carpeta_resultados, nombre_archivo_salida)):
        os.remove(os.path.join(dir_carpeta_resultados, nombre_archivo_salida))

    if dir_subcarpeta == None:
        dir_anexo_3 = dir_carpeta_archivos
    else:
        dir_anexo_3 = os.path.join(dir_carpeta_archivos, dir_subcarpeta)

    print("\n        # -- # -- # Preproceso para Anexo 3 # -- # -- # \n ")
    print("  ------ Concatenacion de anexos 3 \n")
    print(f"        Concatenando archivos en carpeta: {dir_anexo_3}..... \n")

    archivos = os.listdir(dir_anexo_3)
    df = pd.DataFrame(columns=col)
    for archivo in archivos:
        dir_archivo = os.path.join(dir_anexo_3, archivo)
        print(f"Concatenando archivo: {dir_archivo}")
        col_ = col_2019 if re.search(r"(U8|U9|U10|U11|U12|U13)", archivo) else col_2012
        data_temp = pd.read_excel(dir_archivo, header=6, usecols=col_).values
        df = pd.concat([df, pd.DataFrame(columns=col, data=data_temp)])

    df = df.dropna()

    df["id_sentido"] = df["Sentido"].map({"Ida": 1, "Ret": 2})
    df["id_tipo_dia"] = df["Día"].map({"Laboral": 1, "Sábado": 2, "Domingo": 3})
    df.rename(
        columns={
            "Código TS": "id_servicio",
            "MH": "media_hora",
            "Velocidad (Km/hra)": "velocidad",
            "Distancia Base (Km)": "distancia_base",
            "Distancia Total (POB+POI) (Km)": "distancia_integrada",
        },
        inplace=True,
    )
    df = df.loc[
        :,
        [
            "id_servicio",
            "id_sentido",
            "id_tipo_dia",
            "media_hora",
            "velocidad",
            "distancia_base",
            "distancia_integrada",
            "Día",
            "Sentido",
            "N° Salidas",
        ],
    ]
    df.to_excel(
        os.path.join(dir_carpeta_resultados, nombre_archivo_salida), index=False
    )

    if retornar:
        return df
    else:
        return


def concatenar_anexo_8(dir_carpeta_archivos: str, dir_carpeta_resultados: str):
    """Concatena anexos 8 que se encuentra en "dir_carpeta_archivos".

    :param dir_carpeta_archivos: Ruta de la carpeta con carpetas de anexos 4 y 8.
    :type dir_carpeta_archivos: str

    :param dir_carpeta_resultados: Ruta de la carpeta donde se almacenará el resultado.
    :type dir_carpeta_resultados: str

    :return: Archivo ubicado en "~/[dir_carpeta_resultados]/anexo_8_merge.xls" con los anexos 4 y anexos 8 concatenados.
    """

    exist = os.path.exists(dir_carpeta_resultados)
    if not exist:
        os.makedirs(dir_carpeta_resultados)

    nombre_archivo_salida = "anexo_8_merge.xlsx"
    dir_anexos_4 = os.path.join(dir_carpeta_archivos, "anexo_4")
    dir_anexos_8 = os.path.join(dir_carpeta_archivos, "anexo_8")
    tiempo_inicio = time.time()
    print("\n        # -- # -- # Preproceso para Anexo 8 # -- # -- # \n ")
    t_inicio = datetime.datetime.now()
    print(f"  Hora incio ----------------- {t_inicio.time()}")
    print(
        f"  Hora estimada de termino --- {(t_inicio + datetime.timedelta(minutes=31)).time()}\n"
    )
    df = pd.DataFrame(
        columns=["id_servicio", "sentido", "tipo_dia", "hora", "id_tipo_bus"]
    )

    # -- # -- # concatenar anexos 4 # -- # -- #
    print("  ------ Concatenacion de anexos 4 \n")
    archivos = os.listdir(dir_anexos_4)
    dfs = []
    for archivo in archivos:
        dir_archivo = os.path.join(dir_anexos_4, archivo)

        print(f"  Concatenando archivo: {dir_archivo} ")

        df_temp = (
            pd.read_excel(
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
            )
            .rename(
                columns={
                    "CODIGO TS SERVICIO": "id_servicio",
                    "SENTIDO": "sentido",
                    "TIPO_DIA": "tipo_dia",
                    "HORA_INICIO": "hora",
                    "TIPO_BUS": "id_tipo_bus",
                }
            )
            .loc[
                :,
                ["id_servicio", "sentido", "tipo_dia", "hora", "id_tipo_bus"],
            ]
        )
        dfs.append(df_temp)
    df = pd.concat(dfs)

    # -- # -- # concatenar anexos 8 # -- # -- #
    print("\n  ------ Concatenacion de anexos 8 \n")
    archivos = os.listdir(dir_anexos_8)
    for ix, archivo in enumerate(archivos):
        dir_archivo = os.path.join(dir_anexos_8, archivo)

        print(f"  Concatenando archivo: {dir_archivo} ")

        xlsx = pd.ExcelFile(dir_archivo)
        hojas = [
            hoja
            for hoja in xlsx.sheet_names
            if hoja != "Capacidades" and hoja != "Capacidad"
        ]

        for hoja in hojas:
            df = leer_hoja(df, xlsx, hoja, "B:C", "Laboral", "Ida")
            df = leer_hoja(df, xlsx, hoja, "D:E", "Laboral", "Ret")
            df = leer_hoja(df, xlsx, hoja, "F:G", "Laboral", "Ida")
            df = leer_hoja(df, xlsx, hoja, "H:I", "Laboral", "Ret")
            df = leer_hoja(df, xlsx, hoja, "J:K", "Sábado", "Ida")
            df = leer_hoja(df, xlsx, hoja, "L:M", "Sábado", "Ret")
            df = leer_hoja(df, xlsx, hoja, "N:O", "Sábado", "Ida")
            df = leer_hoja(df, xlsx, hoja, "P:Q", "Sábado", "Ret")
            df = leer_hoja(df, xlsx, hoja, "R:S", "Domingo", "Ida")
            df = leer_hoja(df, xlsx, hoja, "T:U", "Domingo", "Ret")
            df = leer_hoja(df, xlsx, hoja, "V:W", "Domingo", "Ida")
            df = leer_hoja(df, xlsx, hoja, "X:Y", "Domingo", "Ret")

    df.dropna(inplace=True)
    if os.path.exists(os.path.join(dir_carpeta_resultados, nombre_archivo_salida)):
        os.remove(os.path.join(dir_carpeta_resultados, nombre_archivo_salida))

    df["id_sentido"] = df["sentido"].map({"Ida": 1, "Ret": 2})
    df["id_tipo_dia"] = df["tipo_dia"].map({"Laboral": 1, "Sábado": 2, "Domingo": 3})
    df = df.loc[:, ["id_servicio", "id_sentido", "id_tipo_dia", "hora", "id_tipo_bus"]]

    df["hora"] = df["hora"].apply(
        lambda x: x.time() if isinstance(x, datetime.datetime) else x
    )

    df.to_excel(
        os.path.join(dir_carpeta_resultados, nombre_archivo_salida), index=False
    )
    tiempo_total = (time.time() - tiempo_inicio) / 60
    print(f"tiempo total: {tiempo_total} minutos")


def concatenar_desde_carpeta(
    carpeta: str,
    nombre_archivo_salida: str,
    tipo: str = "csv",
    hoja: str = None,
    encabezado: int = None,
    decimal: str = None,
    sep: str = None,
):
    """Concatena todos los archivos, que se encuentran en una carpeta, en un solo archivo.

    :param carpeta: Dirección de la carpeta con archivos para concatenar.
    :type carpeta: str

    :param nombre_archivo_salida: Nombre del archivo que contiene los datos de los archivos concatenados.
    :type nombre_archivo_salida: str

    :param tipo: Tipo de archivos a leer (csv o Excel).
    :type tipo: str [opcional]

    :param hoja: Nombre de la hoja de los archivos de Excel a leer.
    :type hoja: str [opcional]

    :param encabezado: Número de fila del Excel desde donde comienza la tabla.
    :type encabezado: int [opcional]


    :return: Archivo guardado en "~/[carpeta]/[nombre_archivo_salida]"con los datos concatenados de los archivos
    """

    if os.path.exists(os.path.join(carpeta, nombre_archivo_salida)):
        os.remove(os.path.join(carpeta, nombre_archivo_salida))
    print(f"\n        Concatenando archivos en carpeta: {carpeta}..... \n")
    dfs = []
    for archivo in os.listdir(carpeta):
        if tipo == "csv":
            dfs.append(
                pd.read_csv(
                    os.path.join(carpeta, archivo), encoding="ISO-8859-1", sep=";"
                )
            )
        elif tipo == "excel":
            if archivo.endswith(".xls"):
                kwargs = {}
            else:
                kwargs = {"engine": "openpyxl"}
            if hoja is not None:
                kwargs["sheet_name"] = hoja
            if encabezado is not None:
                kwargs["header"] = encabezado
            dfs.append(pd.read_excel(os.path.join(carpeta, archivo), **kwargs))
        else:
            raise ValueError("Tipo de archivo no soportado")

    df = pd.concat(dfs, axis=0)
    csv_args = {"index": False}
    if sep is not None:
        csv_args["sep"] = sep
    if decimal is not None:
        csv_args["decimal"] = decimal
    df.to_csv(os.path.join(carpeta, nombre_archivo_salida), **csv_args)
    return


def transformar_anexo_4(
    carpeta: str,
    dir_dic_servicios: str = "diccionario_servicios.xlsx",
    dir_resultados: str = "anexo_4_a_8",
):
    """Transforma los archivos de anexo 4 a anexo 8.

    :param carpeta: ruta de la carpeta con archivos de anexos 4.
    :type carpeta: str

    :param dir_resultados: ruta de la carpeta donde se almacenaran los resultados.
    :type dir_resultados: str [opcional]

    :return: Anexos 4 transformados en Anexos 8 para cada UN en la carpeta "~/carpeta/dir_resultados".
    """

    print("\n        # -- # -- # Anexo 4 ---> Anexo 8 # -- # -- # \n ")
    
    df_servicios = pd.read_excel(dir_dic_servicios, dtype={'cod_ts': str})
    df_servicios.set_index('cod_ts', inplace=True)
    dict_servicios = df_servicios.to_dict('index')

    archivos = [
        x for x in os.listdir(carpeta) if os.path.isfile(os.path.join(carpeta, x))
    ]
    pattern = r"U\d+"
    dir_anexos_4_a_8 = os.path.join(carpeta, dir_resultados)
    if not os.path.exists(dir_anexos_4_a_8):
        os.makedirs(dir_anexos_4_a_8)
    for archivo in archivos:
        print(f"\n  Transformando archivo: {archivo} ....\n")
        dir_archivo = os.path.join(carpeta, archivo)
        un = re.search(pattern, archivo)[0]
        df_temp = (
            pd.read_excel(
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
                dtype={"CODIGO TS SERVICIO": str},
            )
            .rename(
                columns={
                    "CODIGO TS SERVICIO": "id_servicio",
                    "SENTIDO": "sentido",
                    "TIPO_DIA": "tipo_dia",
                    "HORA_INICIO": "hora",
                    "TIPO_BUS": "id_tipo_bus",
                }
            )
            .loc[:, ["id_servicio", "sentido", "tipo_dia", "hora", "id_tipo_bus"]]
        )
        df_temp = df_temp.loc[df_temp["sentido"].isin(["Ida", "Ret"]), :]
        df_temp["hora"] = pd.to_datetime(
            str(datetime.date(2010, 1, 1)) + " " + df_temp["hora"].astype(str),
            utc=pytz.UTC,
        )
        df_temp["codigo_usuario"] = df_temp["id_servicio"].apply(to_cod_usuario, args=(dict_servicios,))
        
        wb = Workbook()
        for servicio in list(df_temp["id_servicio"].unique()):
            escribir_hoja_anexo_8(un, wb, str(servicio), df_temp)
        del wb["Sheet"]
        wb.save(os.path.join(dir_anexos_4_a_8, un + ".xlsx"))

    print("\n        # -- # -- # Terminado!! # -- # -- # \n ")
