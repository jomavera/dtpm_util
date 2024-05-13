import datetime
import os

import numpy as np
import pandas as pd

from dtpm_util.cuadro_de_mando.constantes import MINUTOS

def generar_tabla_temp_1(dir_carpeta_resultados: str):
    """Genera la tabla "temp_1" en base a los archivos "anexo_3_merge.xlsx" y "anexo_8_merge.xlsx" que se encuentran en "dir_carpeta_resultados".

    :param dir_carpeta_resultados: Ruta de la carpeta resultados.
    :type dir_carpeta_resultados: str

    :return: Archivo ubicado en "~/[dir_carpeta_resultados]/temp_1.xlsx".
    """

    exist = os.path.exists(dir_carpeta_resultados)
    if not exist:
        os.makedirs(dir_carpeta_resultados)

    print("        Generando la tabla temporal \n")
    df_anexo_3 = pd.read_excel(
        os.path.join(dir_carpeta_resultados, "anexo_3_merge.xlsx"),
        dtype={"id_servicio": str},
    )
    df_anexo_8 = pd.read_excel(
        os.path.join(dir_carpeta_resultados, "anexo_8_merge.xlsx"),
        dtype={"id_servicio": str},
    )

    df_all = df_anexo_8.merge(
        df_anexo_3, on=["id_tipo_dia", "id_sentido", "id_servicio"], how="left"
    )
    df_all = df_all.loc[~df_all["id_servicio"].str.contains("nc", regex=False), :]
    ids_ceros = df_all.loc[df_all['media_hora'].isna(),'id_servicio'].unique()
    if len(ids_ceros) >0:
        raise Exception(f"Los siguientes servicios {ids_ceros} no presentan información en alguno de los anexos 3 u 8!!!") 
    
    # Crear datetime a partir de hora (tiempo) a nivel de minuto 
    df_all["hora"] = pd.to_datetime(
        str(datetime.date(2010, 1, 1)) + " " + df_all["hora"].astype(str)
    ).dt.floor("min")
    df_all["media_hora"] = pd.to_datetime(
        str(datetime.date(2010, 1, 1)) + " " + df_all["media_hora"].astype(str)
    ).dt.floor("min")
    df_all["media_hora_t"] = df_all["media_hora"] + datetime.timedelta(minutes=29)

    # Seleccionar solo filas que inicio de periodo es menor a la salida y el inicio del siguiente periodo es mayor a la salida
    filas = (df_all["media_hora"] <= df_all["hora"]) & (df_all["media_hora_t"] >= df_all["hora"])

    df_all = df_all.loc[filas, :]

    # Calcular el tiempo que demora la expedicion
    df_all["tiempo_demora"] = 0.0
    df_all.loc[:, "tiempo_demora"] = (
        df_all["distancia_integrada"] / df_all["velocidad"]
    ) * 60
    df_all["tiempo_demora"] = df_all["tiempo_demora"].fillna(0)
    df_all.loc[df_all["tiempo_demora"].apply(np.isinf), "tiempo_demora"] = 0
    df_all["tiempo_demora_td"] = pd.to_timedelta(df_all["tiempo_demora"], "m")

    # Calcular hora de fin de expedicion
    df_all["hora_fin"] = 0
    df_all["hora_fin"] = df_all["hora"] + df_all["tiempo_demora_td"]
    df_all["hora"] = df_all["hora"].dt.time
    df_all["hora_fin"] = df_all["hora_fin"].dt.time

    df_all["hora"] = df_all["hora"].apply(
        lambda x: x.time() if isinstance(x, datetime.datetime) else x
    )
    df_all["hora_fin"] = df_all["hora_fin"].apply(
        lambda x: x.time() if isinstance(x, datetime.datetime) else x
    )

    df_all["hora_fin"] = df_all["hora_fin"].apply(lambda x: str(x)[:8])

    df_all = df_all.loc[
        :,
        [
            "id_tipo_dia",
            "id_sentido",
            "id_servicio",
            "hora",
            "id_tipo_bus",
            "velocidad",
            "distancia_base",
            "distancia_integrada",
            "tiempo_demora",
            "hora_fin",
        ],
    ]

    if os.path.exists(os.path.join(dir_carpeta_resultados, "temp_1.xlsx")):
        os.remove(os.path.join(dir_carpeta_resultados, "temp_1.xlsx"))
    df_all.to_excel(os.path.join(dir_carpeta_resultados, "temp_1.xlsx"), index=False)


def generar_flota(dir_carpeta_resultados: str, archivo_temp_1: str):
    """Genera la tabla "flota" en base a los archivos "temp_1.xlsx" que se encuentra en "dir_carpeta_resultados".

    :param dir_carpeta_resultados: Ruta de la carpeta resultados.
    :type dir_carpeta_resultados: str

    :param archivo_temp_1: Ruta del archivo temp_1.
    :type archivo_temp_1: str

    :return: Archivo ubicado en "~/[dir_carpeta_resultados]/flota.xlsx" con datos de flota cada 10 minutos por servicio, sentido y tipo de día.
    """

    exist = os.path.exists(dir_carpeta_resultados)
    if not exist:
        os.makedirs(dir_carpeta_resultados)

    print("        Generando tabla flota \n")
    df_minutos = pd.DataFrame.from_dict(MINUTOS)
    df_temp_1 = pd.read_excel(archivo_temp_1, dtype={"id_servicio": str})

    df_temp_1["hora"] = pd.to_datetime(
        str(datetime.date(2010, 1, 1)) + " " + df_temp_1["hora"].astype(str)
    )
    df_temp_1["hora_fin"] = pd.to_datetime(
        str(datetime.date(2010, 1, 1)) + " " + df_temp_1["hora_fin"].astype(str)
    )

    df_temp_1["mod_1"] = df_temp_1["hora"].dt.minute.apply(lambda x: x % 10)
    df_temp_1["mod_1"] = pd.to_timedelta(df_temp_1["mod_1"], "m")
    df_temp_1["hora_10min"] = df_temp_1["hora"] - df_temp_1["mod_1"]
    df_temp_1["hora_10min"] = df_temp_1["hora_10min"].dt.floor("T")

    df_temp_1["mod_2"] = df_temp_1["hora_fin"].dt.minute.apply(lambda x: x % 10)
    df_temp_1["mod_2"] = pd.to_timedelta(df_temp_1["mod_2"], "m")
    df_temp_1["hora_fin_10min"] = df_temp_1["hora_fin"] - df_temp_1["mod_2"]
    df_temp_1["hora_fin_10min"] = df_temp_1["hora_fin_10min"].dt.floor("T")

    df_all = df_minutos.merge(df_temp_1, how="cross")

    df_all["minuto"] = pd.to_datetime(
        str(datetime.date(2010, 1, 1)) + " " + df_all["minuto"].astype(str)
    )

    df_all["diff_1"] = (
        df_all["hora_10min"] - df_all["minuto"]
    ).dt.total_seconds() / 60.0
    df_all["diff_2"] = (
        df_all["hora_fin_10min"] - df_all["minuto"]
    ).dt.total_seconds() / 60.0

    df_all = df_all.loc[(df_all["diff_1"] <= 0) & (df_all["diff_2"] >= 0), :]
    df_all["minuto"] = df_all["minuto"].apply(
        lambda x: x.time() if isinstance(x, datetime.datetime) else x
    )

    df_c = df_all.value_counts(
        ["minuto", "id_servicio", "id_tipo_dia", "id_sentido"]
    ).reset_index()
    df_c = df_c.rename(columns={'count': "flota"})
    if os.path.exists(os.path.join(dir_carpeta_resultados, "flota.xlsx")):
        os.remove(os.path.join(dir_carpeta_resultados, "flota.xlsx"))
    df_c.to_excel(os.path.join(dir_carpeta_resultados, "flota.xlsx"), index=False)


def generar_flota_mh(dir_carpeta_resultados: str, archivo_flota: str):
    """Genera flota por media hora, servicio y sentido en base al tabla "flota" que se encuentra en "dir_carpeta_resultados".

    :param dir_carpeta_resultados: Ruta de la carpeta resultados.
    :type dir_carpeta_resultados: str

    :param archivo_flota: Ruta del archivo con datos de flota.
    :type archivo_flota: str

    :return: Archivo ubicado en "~/[dir_carpeta_resultados]/flota_mh.xlsx" con datos de flota cada 30 minutos por servicio, sentido y tipo de dia.

    """

    exist = os.path.exists(dir_carpeta_resultados)
    if not exist:
        os.makedirs(dir_carpeta_resultados)

    print("        Generando la tabla flota mh \n")
    df_flota = pd.read_excel(archivo_flota)
    df_flota["minuto"] = pd.to_datetime(
        str(datetime.date(2010, 1, 1)) + " " + df_flota["minuto"].astype(str)
    )

    df_flota["media_hora"] = df_flota["minuto"].dt.floor("30T").dt.time

    df_flota_gb = (
        df_flota[["media_hora", "id_servicio", "id_sentido", "id_tipo_dia", "flota"]]
        .groupby(
            ["media_hora", "id_servicio", "id_sentido", "id_tipo_dia"], as_index=False
        )
        .sum()
    )
    if os.path.exists(os.path.join(dir_carpeta_resultados, "flota_mh.xlsx")):
        os.remove(os.path.join(dir_carpeta_resultados, "flota_mh.xlsx"))
    df_flota_gb.to_excel(
        os.path.join(dir_carpeta_resultados, "flota_mh.xlsx"), index=False
    )


def generar_ict(
    dir_carpeta_resultados: str,
    archivo_flota: str,
    archivo_perfil: str,
    archivo_flota_mh: str,
):
    """Cacular ICT a partir de datos de flota por hora, servicio y sentido, perfilt teorico.

    :param dir_carpeta_resultados: Ruta de la carpeta resultados.
    :type dir_carpeta_resultados: str

    :param archivo_flota: Ruta del archivo con datos de flota.
    :type archivo_flota: str

    :param archivo_perfil: Ruta del archivo con datos de perfil teórico.
    :type archivo_perfil: str

    :param archivo_flota_mh: Ruta del archivo con datos de flota cada media hora.
    :type archivo_flota_mh: str

    :return: Archivo ubicado en "~/[dir_carpeta_resultados]/ict.xlsx" con datos de ict cada 10 minutos por servicio, sentido y tipo de dia.

    """

    exist = os.path.exists(dir_carpeta_resultados)
    if not exist:
        os.makedirs(dir_carpeta_resultados)

    print("        Generando tabla ict \n")
    df_flota = pd.read_excel(archivo_flota, dtype={"id_servicio": str})
    df_flota["id_servicio"] = df_flota["id_servicio"].str.upper()
    df_perfil = pd.read_excel(archivo_perfil, dtype={"id_servicio": str})
    df_perfil["id_servicio"] = df_perfil["id_servicio"].str.upper()
    df_flota_mh = pd.read_excel(archivo_flota_mh, dtype={"id_servicio": str})
    df_flota_mh["id_servicio"] = df_flota_mh["id_servicio"].str.upper()
    df_flota_mh.rename(columns={"flota": "flota_mh"}, inplace=True)

    df_all = df_flota.merge(
        df_perfil, on=["id_servicio", "id_sentido", "id_tipo_dia"], how="left"
    )

    df_all["minuto"] = pd.to_datetime(
        str(datetime.date(2010, 1, 1)) + " " + df_all["minuto"].astype(str)
    )
    ids_ceros = df_all.loc[df_all['media_hora'].isna(),'id_servicio'].unique()
    if len(ids_ceros) >0:
        raise Exception(f"Los siguientes servicios {ids_ceros} no presentan información en el perfil teorico !!!") 

    df_all["media_hora"] = pd.to_datetime(
        str(datetime.date(2010, 1, 1)) + " " + df_all["media_hora"].astype(str)
    )
    df_all["media_hora_t"] = df_all["media_hora"] + datetime.timedelta(minutes=29)

    df_all["diff_hora"] = df_all["media_hora"] - df_all["minuto"]
    df_all["diff_hora"] = df_all["diff_hora"].dt.total_seconds() / 60

    df_all["diff_hora_t"] = df_all["media_hora_t"] - df_all["minuto"]
    df_all["diff_hora_t"] = df_all["diff_hora_t"].dt.total_seconds() / 60

    df_all = df_all.loc[
        (df_all["diff_hora"] <= 0) & (df_all["diff_hora_t"] >= 0),
        ["id_servicio", "id_sentido", "id_tipo_dia", "minuto", "perfil", "flota"],
    ]

    df_all = df_all.merge(df_flota_mh, on=["id_servicio", "id_sentido", "id_tipo_dia"])
    df_all["media_hora"] = pd.to_datetime(
        str(datetime.date(2010, 1, 1)) + " " + df_all["media_hora"].astype(str)
    )
    df_all["media_hora_t"] = df_all["media_hora"] + datetime.timedelta(minutes=29)

    df_all["diff_hora"] = df_all["media_hora"] - df_all["minuto"]
    df_all["diff_hora"] = df_all["diff_hora"].dt.total_seconds() / 60

    df_all["diff_hora_t"] = df_all["media_hora_t"] - df_all["minuto"]
    df_all["diff_hora_t"] = df_all["diff_hora_t"].dt.total_seconds() / 60

    filas = (df_all["diff_hora"] <= 0) & (df_all["diff_hora_t"] >= 0)

    df_all = df_all.loc[filas, :]

    df_all["ict"] = (df_all["flota"] / df_all["flota_mh"]) * df_all["perfil"]

    df_all = df_all.loc[
        :, ["id_servicio", "id_sentido", "id_tipo_dia", "minuto", "ict"]
    ]
    df_all["minuto"] = df_all["minuto"].dt.time

    if os.path.exists(os.path.join(dir_carpeta_resultados, "ict.xlsx")):
        os.remove(os.path.join(dir_carpeta_resultados, "ict.xlsx"))
    df_all.to_excel(os.path.join(dir_carpeta_resultados, "ict.xlsx"), index=False)


def generar_icf(dir_carpeta_resultados: str):
    """Calcular ICF a partir del archivos "anexo_8_merge.xlsx" que se encuentra en la carpeta "dir_carpeta_resultados".

    :param dir_carpeta_resultados: Ruta de la carpeta donde se encuentran los archivos de resultados.
    :type dir_carpeta_resultados: str

    :return: Archivo ubicado en "~/[dir_carpeta_resultados]/icf.xlsx" con datos de icf cada 10 minutos por servicio, sentido y tipo de dia.

    """

    print("        Generando tabla icf \n")
    df_anexo_8 = pd.read_excel(
        os.path.join(dir_carpeta_resultados, "anexo_8_merge.xlsx")
    )
    df_anexo_8["hora"] = pd.to_datetime(
        str(datetime.date(2010, 1, 1)) + " " + df_anexo_8["hora"].astype(str)
    )
    df_anexo_8["minuto"] = df_anexo_8["hora"].dt.minute.apply(lambda x: x % 10)
    df_anexo_8["minuto"] = df_anexo_8["hora"] - pd.to_timedelta(
        df_anexo_8["minuto"], "m"
    )
    df_anexo_8["minuto"] = df_anexo_8["minuto"].dt.strftime("%H:%M")

    df_vc = (
        df_anexo_8.value_counts(["id_servicio", "id_sentido", "id_tipo_dia", "minuto"])
        .reset_index()
        .rename(columns={'count': "salida"})
    )

    df_vc.to_excel(os.path.join(dir_carpeta_resultados, "icf.xlsx"), index=False)


def formato_flota(dir_carpeta_resultados: str, archivo_flota: str):
    """Cambiar formato de tabla flota según especificaciones SIG y generar archivo "formato_flota.xlsx" en carpeta "dir_carpeta_resultados".

    :param dir_carpeta_resultados: Ruta de la carpeta donde se encuentran los archivos de resultados.
    :type dir_carpeta_resultados: str

    :param archivo_flota: Ruta del archivo de flota.
    :type archivo_flota: str

    :return: Archivo ubicado en "~/[dir_carpeta_resultados]/formato_flota.xlsx" con datos de flota (pivoteada) cada 10 minutos por servicio, sentido y tipo de dia.

    """

    print("        Pivoteando la tabla flota \n")
    df_flota = pd.read_excel(archivo_flota)
    df_flota = df_flota.loc[~df_flota["id_servicio"].str.contains("nc"), :]
    df_flota = df_flota.groupby(
        ["id_servicio", "id_sentido", "id_tipo_dia", "minuto"], as_index=False
    ).sum()

    df_flota_pivot = df_flota.pivot(
        index=["id_servicio", "id_sentido", "id_tipo_dia"],
        columns="minuto",
        values="flota",
    ).reset_index()
    df_flota_pivot["id_sentido"] = df_flota_pivot["id_sentido"].map({1: "I", 2: "R"})
    df_flota_pivot.rename(
        columns={
            "id_servicio": "Servicio",
            "id_sentido": "Sentido",
            "id_tipo_dia": "Tipo de dia",
        },
        inplace=True,
    )
    if os.path.exists(os.path.join(dir_carpeta_resultados, "formato_flota.xlsx")):
        os.remove(os.path.join(dir_carpeta_resultados, "formato_flota.xlsx"))
    df_flota_pivot.to_excel(
        os.path.join(dir_carpeta_resultados, "formato_flota.xlsx"), index=False
    )


def formato_ict(dir_carpeta_resultados: str, archivo_ict: str):
    """Cambiar formato de tabla ict según especificaciones SIG y generar archivo "formato_ict.xlsx" en carpeta "dir_carpeta_resultados".

    :param dir_carpeta_resultados: Ruta de la carpeta donde se encuentran los archivos de resultados.
    :type dir_carpeta_resultados: str

    :param archivo_ict: Ruta del archivo ICT.
    :type archivo_ict: str

    :return: Archivo ubicado en "~/[dir_carpeta_resultados]/formato_flota.xlsx" con datos de ict (pivoteada) cada 10 minutos por servicio, sentido y tipo de día.

    """

    print("        Pivoteando la tabla ict \n")
    df_ict = pd.read_excel(archivo_ict)
    df_ict = df_ict.loc[~df_ict["id_servicio"].str.contains("nc"), :]
    df_ict = df_ict.groupby(
        ["id_servicio", "id_sentido", "id_tipo_dia", "minuto"], as_index=False
    ).sum()
    df_ict["ict"] = df_ict["ict"].map(round)
    df_ict_pivot = df_ict.pivot(
        index=["id_servicio", "id_sentido", "id_tipo_dia"],
        columns="minuto",
        values="ict",
    ).reset_index()
    df_ict_pivot["id_sentido"] = df_ict_pivot["id_sentido"].map({1: "I", 2: "R"})
    df_ict_pivot.rename(
        columns={
            "id_servicio": "Servicio",
            "id_sentido": "Sentido",
            "id_tipo_dia": "Tipo de dia",
        },
        inplace=True,
    )
    if os.path.exists(os.path.join(dir_carpeta_resultados, "formato_ict.xlsx")):
        os.remove(os.path.join(dir_carpeta_resultados, "formato_ict.xlsx"))
    df_ict_pivot.to_excel(
        os.path.join(dir_carpeta_resultados, "formato_ict.xlsx"), index=False
    )


def formato_icf(dir_carpeta_resultados: str, archivo_icf: str):
    """Cambiar formato de tabla icf según especificaciones SIG y generar archivo "formato_icf.xlsx" en carpeta "dir_carpeta_resultados".

    :param dir_carpeta_resultados: Ruta de la carpeta donde se encuentran los archivos de resultados.
    :type dir_carpeta_resultados: str

    :param archivo_icf: Ruta del archivo ICF.
    :type archivo_icf: str

    :return: Archivo ubicado en "~/[dir_carpeta_resultados]/formato_flota.xlsx" con datos de icf (pivoteada) cada 10 minutos por servicio, sentido y tipo de dia.

    """
    print("        Pivoteando la tabla icf \n")
    df_icf = pd.read_excel(archivo_icf, dtype={"id_servicio": str})
    df_icf = df_icf.loc[~df_icf["id_servicio"].str.contains("nc"), :]
    df_icf = df_icf.groupby(
        ["id_servicio", "id_sentido", "id_tipo_dia", "minuto"], as_index=False
    ).mean()

    df_icf_pivot = df_icf.pivot(
        index=["id_servicio", "id_sentido", "id_tipo_dia"],
        columns="minuto",
        values="salida",
    ).reset_index()
    df_icf_pivot["id_sentido"] = df_icf_pivot["id_sentido"].map({1: "I", 2: "R"})
    df_icf_pivot.rename(
        columns={
            "id_servicio": "Servicio",
            "id_sentido": "Sentido",
            "id_tipo_dia": "Tipo de dia",
        },
        inplace=True,
    )

    if os.path.exists(os.path.join(dir_carpeta_resultados, "formato_icf.xlsx")):
        os.remove(os.path.join(dir_carpeta_resultados, "formato_icf.xlsx"))
    df_icf_pivot.to_excel(
        os.path.join(dir_carpeta_resultados, "formato_icf.xlsx"), index=False
    )


def codificar_perfil(archivo_perfil: str, dir_carpeta_resultados: str):
    """Codificar "sentido" y "tipo de dia" del perfil teórico según especificaciones SIG y generar archivo "perfil.xlsx" en carpeta dir_carpeta_resultados.

    :param archivo_perfil: Ruta del archivo del perfil teórico.
    :type archivo_perfil: str

    :param dir_carpeta_resultados: Ruta de la carpeta donde se encuentran los archivos de resultados.
    :type dir_carpeta_resultados: str

    :return: Archivo ubicado en "~/[dir_carpeta_resultados]/perfil.xlsx" del perfil teorico con codificaciones de sentido y tipo de día.

    """

    print("        Renombrando valores del perfil teórico\n")
    df = pd.read_excel(archivo_perfil)
    df["id_sentido"] = df["sentido"].map({"Ida": 1, "Ret": 2})
    df["id_tipo_dia"] = df["tipo dia"].map({"Laboral": 1, "Sábado": 2, "Domingo": 3})
    df.rename(columns={"servicio": "id_servicio"}, inplace=True)
    if os.path.exists(os.path.join(dir_carpeta_resultados, "perfil.xlsx")):
        os.remove(os.path.join(dir_carpeta_resultados, "perfil.xlsx"))
    df.to_excel(os.path.join(dir_carpeta_resultados, "perfil.xlsx"), index=False)

def generar_icf_mh(dir_carpeta_resultados: str):
    """Calcular ICF a partir del archivos "anexo_8_merge.xlsx" que se encuentra en la carpeta dir_carpeta_resultados.

    :param dir_carpeta_resultados: Ruta de la carpeta donde se encuentran los archivos de resultados.
    :type dir_carpeta_resultados: str

    :return: Archivo ubicado en "~/[dir_carpeta_resultados]/icf.xlsx" con datos de icf cada 10 minutos por servicio, sentido y tipo de día.

    """

    print("        Generando tabla icf mh \n")
    df_anexo_8 = pd.read_excel(
        os.path.join(dir_carpeta_resultados, "anexo_8_merge.xlsx")
    )
    df_anexo_8["hora"] = pd.to_datetime(
        str(datetime.date(2010, 1, 1)) + " " + df_anexo_8["hora"].astype(str)
    )
    df_anexo_8["minuto"] = df_anexo_8["hora"].dt.minute.apply(lambda x: x % 30)
    df_anexo_8["minuto"] = df_anexo_8["hora"] - pd.to_timedelta(
        df_anexo_8["minuto"], "m"
    )
    df_anexo_8["minuto"] = df_anexo_8["minuto"].dt.strftime("%H:%M")

    df_vc = (
        df_anexo_8.value_counts(["id_servicio", "id_sentido", "id_tipo_dia", "minuto"])
        .reset_index()
        .rename(columns={'count': "salida"})
    )

    df_vc.to_excel(os.path.join(dir_carpeta_resultados, "icf_mh.xlsx"), index=False)
