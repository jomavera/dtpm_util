import datetime
import os
from typing import List

import pandas as pd
import pytz


def abrir_archivo(
    archivo: str, tipo: str, carpeta: str = None, mensaje_error: str = "No se encontró el archivo", sep: str = None
):
    """Escribe una hoja de Excel con los datos de un DataFrame de pandas.

    :param archivo: Nombre del archivo a leer.
    :type archivo: str

    :param tipo: Extension del archivo a leer.
    :type tipo: str

    :param carpeta: Ruta de la carpeta donde se encuentra el archivo.
    :type carpeta: str [opcional]

    :param mensaje_error: Mensaje de error a mostrar si no se encuentra el archivo.
    :type mensaje_error: str [opcional]

    :return: pandas.DataFrame con los datos del archivo abierto.
    :rtype: pandas.core.frame.DataFrame
    """
    if carpeta != None:
        dir_archivo = os.path.join(carpeta, archivo)
    else:
        dir_archivo = archivo
    if os.path.exists(dir_archivo):
        print(f"        Cargando archivo {dir_archivo}....\n")
        if tipo == "xlsx":
            df = pd.read_excel(dir_archivo)
        elif tipo == 'csv':
            if sep != None:
                df = pd.read_csv(dir_archivo, dayfirst=True, sep=sep)
            elif sep == None:
                df = pd.read_csv(dir_archivo, dayfirst=True)
        return df
    else:
        raise NameError(mensaje_error)


def escribir_hoja_anexo_8(
    unidad_de_negocio: str, excel: pd.ExcelFile, hoja: str, df: str
):
    """Escribe una hoja de Excel con los datos de un DataFrame de pandas.

    :param unidad_de_negocio: Unidad de negocio del anexo 8.
    :type unidad_de_negocio: str

    :param excel: Objecto de ExcelFile del cual se quiere concatenar los datos de la hoja.
    :type excel: pandas.ExcelFile

    :param hoja: nombre de la hoja (id_servicio) con que se quiere escribir hoja de Excel.
    :type hoja: str

    :param df: DataFrame a añadirse los datos de la hoja de Excel.
    :type df: pandas.core.frame.DataFrame

    :return: Guarda datos en la hoja de Excel
    """
    cod_usuario = list(df.loc[(df["id_servicio"] == hoja),'codigo_usuario'].unique())[0]
    ws = excel.create_sheet(title=hoja)
    ws["A1"] = "UNIDAD DE NEGOCIO"
    ws["A2"] = "CODIGO TS"
    ws["A3"] = "CÓDIGO USUARIO"
    ws["D1"] = unidad_de_negocio[1:]
    ws["D2"] = hoja
    ws["D3"] = cod_usuario

    ws["B7"] = "Laboral"
    ws["B8"] = "Diurno"
    ws["B9"] = "IDA"
    ws["C9"] = "TB"
    ws["D9"] = "RET"
    ws["E9"] = "IDA"
    ws["F7"] = "Laboral"
    ws["F8"] = "Nocturno"
    ws["F9"] = "IDA"
    ws["G9"] = "TB"
    ws["H9"] = "RET"
    ws["I9"] = "IDA"
    ws["J7"] = "Sábado"
    ws["J8"] = "Diurno"
    ws["J9"] = "IDA"
    ws["K9"] = "TB"
    ws["L9"] = "RET"
    ws["M9"] = "IDA"
    ws["N7"] = "Sábado"
    ws["N8"] = "Nocturno"
    ws["N9"] = "IDA"
    ws["O9"] = "TB"
    ws["P9"] = "RET"
    ws["Q9"] = "IDA"
    ws["R7"] = "Domingo"
    ws["R8"] = "Diurno"
    ws["R9"] = "IDA"
    ws["S9"] = "TB"
    ws["T9"] = "RET"
    ws["U9"] = "IDA"
    ws["V7"] = "Domingo"
    ws["V8"] = "Nocturno"
    ws["V9"] = "IDA"
    ws["W9"] = "TB"
    ws["X9"] = "RET"
    ws["Y9"] = "IDA"

    dict_col = {
        "Laboral_Ida_diurno": ("B", "C"),
        "Laboral_Ret_diurno": ("D", "E"),
        "Laboral_Ida_nocturno": ("F", "G"),
        "Laboral_Ret_nocturno": ("H", "I"),
        "Sábado_Ida_diurno": ("J", "K"),
        "Sábado_Ret_diurno": ("L", "M"),
        "Sábado_Ida_nocturno": ("N", "O"),
        "Sábado_Ret_nocturno": ("P", "Q"),
        "Domingo_Ida_diurno": ("R", "S"),
        "Domingo_Ret_diurno": ("T", "U"),
        "Domingo_Ida_nocturno": ("V", "W"),
        "Domingo_Ret_nocturno": ("X", "Y"),
    }

    for tipo_dia in ["Laboral", "Sábado", "Domingo"]:
        for sentido in ["Ida", "Ret"]:
            for periodo in ["diurno", "nocturno"]:
                if periodo == "diurno":
                    hora_inicio = datetime.datetime(
                        2010, 1, 1, 5, 30, 0, tzinfo=pytz.UTC
                    )
                    hora_fin = datetime.datetime(
                        2010, 1, 1, 23, 59, 59, tzinfo=pytz.UTC
                    )
                else:
                    hora_inicio = datetime.datetime(
                        2010, 1, 1, 0, 0, 0, tzinfo=pytz.UTC
                    )
                    hora_fin = datetime.datetime(2010, 1, 1, 5, 59, 59, tzinfo=pytz.UTC)

                df_temp = df.loc[
                    (df["id_servicio"] == hoja)
                    & (df["tipo_dia"] == tipo_dia)
                    & (df["sentido"] == sentido)
                    & (df["hora"] >= hora_inicio)
                    & (df["hora"] <= hora_fin),
                    :,
                ]

                col_hora = dict_col[tipo_dia + "_" + sentido + "_" + periodo][0]
                col_tb = dict_col[tipo_dia + "_" + sentido + "_" + periodo][1]
                fila = 10
                contador = 0
                for ix, row in df_temp.iterrows():
                    ws[col_hora + str(fila + contador)] = row["hora"].time()
                    ws[col_tb + str(fila + contador)] = row["id_tipo_bus"]
                    contador += 1


def leer_hoja(
    df: pd.DataFrame,
    archivo: str,
    hoja: str,
    columnas: List[str],
    tipo_dia: str,
    sentido: str,
    header: int = 8,
    fecha_po = None
):
    """Lee hora una hoja de Excel y concatena los datos a un DataFrame de pandas

    :param df: DataFrame a añadirse los datos de la hoja de Excel.
    :type df: pandas.core.frame.DataFrame

    :param archivo: Ruta del archivo a extraerse los datos.
    :type archivo: str

    :param hoja: Nombre de la hoja del archivo a extraerse los datos.
    :type hoja: str

    :param columnas: Nombres de las columnas a leerse.
    :type columnas: list[str]

    :param tipo_dia: Tipo de dia de las salidas de las expediciones descritas en la hoja de Excel.
    :type tipo_dia: str

    :param sentido: Sentido de las salidas de las expediciones descritas en la hoja de Excel.
    :type sentido: str

    :param header: Número de fila desde donde empieza la tabla en la hoja de Excel.
    :type header: int

    

    :return: pandas DataFrame con los datos adjuntados de la hoja de Excel leída.
    :rtype: pandas.core.frame.DataFrame
    """
    df_all = df.copy()
    df_temp = pd.read_excel(archivo, hoja, header=header, usecols=columnas)

    for ix, fila in df_temp.iterrows():
        if fecha_po != None:
            serie = pd.Series(
                {
                    "id_servicio": str(hoja),
                    "sentido": sentido,
                    "tipo_dia": tipo_dia,
                    "hora": fila.iloc[0],
                    "id_tipo_bus": fila.iloc[1],
                    "fecha_po": fecha_po
                }
            )
        else:
            serie = pd.Series(
                {
                    "id_servicio": str(hoja),
                    "sentido": sentido,
                    "tipo_dia": tipo_dia,
                    "hora": fila.iloc[0],
                    "id_tipo_bus": fila.iloc[1]
                }
            )            

        df_all = pd.concat([df_all, serie.to_frame().T], ignore_index=True)

    return df_all

def leer_hoja_servicios(
    df: pd.DataFrame,
    archivo: str,
    hoja: str,
    columnas: List[str],
    tipo_dia: str,
    sentido: str,
    fecha_inicio:str,
    fecha_fin:str,
    un:str,
    header: int = 8,
):
    """Lee hora una hoja de Excel y concatena los datos a un DataFrame de pandas

    :param df: DataFrame a añadirse los datos de la hoja de Excel.
    :type df: pandas.core.frame.DataFrame

    :param archivo: Ruta del archivo a extraerse los datos.
    :type archivo: str

    :param hoja: Nombre de la hoja del archivo a extraerse los datos.
    :type hoja: str

    :param columnas: Nombres de las columnas a leerse.
    :type columnas: list[str]

    :param tipo_dia: Tipo de dia de las salidas de las expediciones descritas en la hoja de Excel.
    :type tipo_dia: str

    :param sentido: Sentido de las salidas de las expediciones descritas en la hoja de Excel.
    :type sentido: str

    :param header: Número de fila desde donde empieza la tabla en la hoja de Excel.
    :type header: int

    :return: pandas DataFrame con los datos adjuntados de la hoja de Excel leída.
    :rtype: pandas.core.frame.DataFrame
    """
    df_temp = pd.read_excel(archivo, hoja, header=header, usecols=columnas)

    for ix, fila in df_temp.iterrows():
        serie = pd.Series(
            {
                "Fecha Inicio":fecha_inicio,
                "Fecha Fin":fecha_fin,
                "UN":un,
                "tipo_dia": tipo_dia,
                "id_servicio": str(hoja),
                "sentido": sentido
            }
        )       
        df_all = pd.concat([df, serie.to_frame().T], ignore_index=True)
        break

    return df_all

