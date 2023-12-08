import os

import pandas as pd

from dtpm_util.cuadro_de_mando.operaciones import (
    generar_flota,
    generar_flota_mh,
    generar_icf,
    generar_icf_mh,
    generar_tabla_temp_1,
)
from dtpm_util.utilidades.preprocesamiento import (
    concatenar_anexo_3,
    concatenar_anexo_8,
)

from dtpm_util.utilidades.transformaciones import to_unidad


def calculo_flota_salidas(
    carpeta_anexos: str,
    dir_carpeta_resultados: str = "resultados_flota_salidas",
    dir_dic_periodos: str = "diccionario_periodos.xlsx",
    dir_dic_servicios: str = "diccionario_servicios.xlsx",
    concatenar_anexos: bool = True,
):
    """Calcula la flota y salidas cada 10/30 min según programa de operación.

    :param carpeta_anexos: Ruta de la carpeta donde se encuentran los anexos 3 y 8.
    :type carpeta_anexos: str

    :param dir_carpeta_resultados: Ruta de la carpeta donde se guardaran los resultados.
    :type dir_carpeta_resultados: str [opcional]

    :param dir_dic_periodos: Ruta del archivo de diccionario de periodos.
    :type dir_dic_periodos: str [opcional]

    :param concatenar_anexos: Booleano que indica si se concatenan los anexos 3 y 8.
    :type concatenar_anexos: bool [opcional]

    :return: Genera dos archivos excel con la flota y salidas en dir_carpeta_resultados.

    :Example:
    calculos.calculo_flota_salidas(
        carpeta_anexos='anexos',
        archivo_diez_minuto='diez_minutos.xlsx',
        dir_carpeta_resultados='resultados',
        concatenar_datos=True
    )

    """

    exist = os.path.exists(dir_carpeta_resultados)
    if not exist:
        os.makedirs(dir_carpeta_resultados)

    df_servicios = pd.read_excel(dir_dic_servicios, dtype={'cod_ts': str})
    df_servicios.set_index('cod_ts', inplace=True)
    dict_servicios = df_servicios.to_dict('index')

    if concatenar_anexos:
        concatenar_anexo_3(carpeta_anexos, dir_carpeta_resultados, "anexo_3")
        concatenar_anexo_8(carpeta_anexos, dir_carpeta_resultados)

    generar_tabla_temp_1(dir_carpeta_resultados)

    generar_flota(
        dir_carpeta_resultados,
        os.path.join(dir_carpeta_resultados, "temp_1.xlsx"),
    )
    generar_flota_mh(
        dir_carpeta_resultados, os.path.join(dir_carpeta_resultados, "flota.xlsx")
    )
    # añadir columna fecha en tabla flota
    df_flota = pd.read_excel(os.path.join(dir_carpeta_resultados, "flota.xlsx"))
    df_flota["id_tipo_dia"] = df_flota["id_tipo_dia"].replace(
        {1: "Laboral", 2: "Sábado", 3: "Domingo"}
    )
    df_flota["id_sentido"] = df_flota["id_sentido"].replace({1: "Ida", 2: "Retorno"})
    df_flota["id_unidad"] = df_flota["id_servicio"].apply(to_unidad, args=(dict_servicios,))

    df_periodos = pd.read_excel(dir_dic_periodos)
    df_periodos["Hora_inicio"] = pd.to_datetime(
        df_periodos["Hora_inicio"], format="%H:%M:%S"
    )
    df_periodos["Hora_fin"] = pd.to_datetime(df_periodos["Hora_fin"], format="%H:%M:%S")
    df_flota["minuto"] = pd.to_datetime(df_flota["minuto"], format="%H:%M:%S")
    df_flota = df_flota.merge(
        df_periodos, left_on=["id_tipo_dia"], right_on=["Tipo_de_dia"], how="inner"
    )
    df_flota = df_flota.loc[
        (df_flota["minuto"] >= df_flota["Hora_inicio"])
        & (df_flota["minuto"] < df_flota["Hora_fin"]),
        :,
    ]
    df_flota = df_flota[
        [
            "minuto",
            "id_servicio",
            "id_tipo_dia",
            "id_sentido",
            "flota",
            "id_unidad",
            "Nombre_descripcion",
        ]
    ]
    df_flota.drop_duplicates(inplace=True)
    df_flota["minuto"] = df_flota["minuto"].dt.strftime("%H:%M")
    df_flota.to_excel(os.path.join(dir_carpeta_resultados, "flota.xlsx"), index=False)

    generar_icf(dir_carpeta_resultados)
    generar_icf_mh(dir_carpeta_resultados)

    # añadir columna fecha en tabla salidas
    df_salidas = pd.read_excel(os.path.join(dir_carpeta_resultados, "icf.xlsx"))
    df_salidas["id_tipo_dia"] = df_salidas["id_tipo_dia"].replace(
        {1: "Laboral", 2: "Sábado", 3: "Domingo"}
    )
    df_salidas["id_sentido"] = df_salidas["id_sentido"].replace(
        {1: "Ida", 2: "Retorno"}
    )
    
    df_salidas["id_unidad"] = df_salidas["id_servicio"].apply(to_unidad, args=(dict_servicios,))
    df_salidas.to_excel(
        os.path.join(dir_carpeta_resultados, "salidas.xlsx"), index=False
    )

    df_salidas_mh = pd.read_excel(os.path.join(dir_carpeta_resultados, "icf_mh.xlsx"))
    df_salidas_mh["id_tipo_dia"] = df_salidas_mh["id_tipo_dia"].replace(
        {1: "Laboral", 2: "Sábado", 3: "Domingo"}
    )
    df_salidas_mh["id_sentido"] = df_salidas_mh["id_sentido"].replace(
        {1: "Ida", 2: "Retorno"}
    )
    df_salidas_mh["id_unidad"] = df_salidas_mh["id_servicio"].apply(to_unidad, args=(dict_servicios,))
    df_salidas_mh["minuto"] = pd.to_datetime(df_salidas_mh["minuto"], format="%H:%M")
    df_salidas_mh = df_salidas_mh.merge(
        df_periodos, left_on=["id_tipo_dia"], right_on=["Tipo_de_dia"], how="inner"
    )
    df_salidas_mh = df_salidas_mh.loc[
        (df_salidas_mh["minuto"] >= df_salidas_mh["Hora_inicio"])
        & (df_salidas_mh["minuto"] < df_salidas_mh["Hora_fin"]),
        :,
    ]
    df_salidas_mh = df_salidas_mh[
        [
            "minuto",
            "id_servicio",
            "id_tipo_dia",
            "id_sentido",
            "salida",
            "id_unidad",
            "Nombre_descripcion",
        ]
    ]
    df_salidas_mh.drop_duplicates(inplace=True)
    df_salidas_mh["minuto"] = df_salidas_mh["minuto"].dt.strftime("%H:%M")
    df_salidas_mh.to_excel(
        os.path.join(dir_carpeta_resultados, "salidas_mh.xlsx"), index=False
    )
