import pandas as pd

from dtpm_util.utilidades.transformaciones import a_datetime, excel_date, make_delta


def transformar_anexo_3(
    fecha: str, df: pd.DataFrame, diccionario: str, es_un: bool = False
):
    """Transforma anexo 3 para cruzar con eventos exogenos.

    :param fecha: Texto con formato "hh:mm:ss".
    :type fecha: str

    :param df: Dataframe con datos del anexo 3.
    :type df: pandas.core.frame.DataFrame

    :param diccionario: Ruta del archivo del diccionario de periodos.
    :type diccionario: str

    :param es_un: Indica si el anexo 3 es de unidades de servicio.
    :type es_un: bool [opcional]

    :return: Anexo 3 transformado.
    :rtype: pandas.core.frame.DataFrame
    """
    df = df.astype({"media_hora": str, "id_servicio": str})
    df_dic = pd.read_excel(diccionario, dtype={"Hora inicio": str})

    df = df.merge(
        df_dic,
        left_on=["media_hora", "Día"],
        right_on=["Hora inicio", "Tipo Día"],
        how="left",
    )

    df["PO"] = fecha
    df["Fecha"] = pd.to_datetime(df["PO"], format="%d-%m-%Y")
    df["Fecha"] = df["Fecha"].apply(lambda x: excel_date(x))
    df["Sentido"] = df["Sentido"].map({"Ida": "I", "Regreso": "R", "Ret": "R"})

    df["POSSPD"] = (
        df["Fecha"].apply(lambda x: str(x))
        + "-"
        + df["id_servicio"]
        + "-"
        + df["Sentido"].apply(lambda x: str(x))
        + "-"
        + df["Numero de Periodo"].apply(lambda x: str(x))
    )

    df = df.loc[df["velocidad"] != 0]

    if es_un:
        df.rename(
            columns={
                "Numero de Periodo": "Periodo",
                "N° Salidas": "SumaExpediciones",
                "velocidad": "PromVelocidad",
                "id_servicio": "Servicio",
            },
            inplace=True,
        )
        return (
            df[
                [
                    "POSSPD",
                    "PO",
                    "Servicio",
                    "Sentido",
                    "Periodo",
                    "Día",
                    "SumaExpediciones",
                    "PromVelocidad",
                ]
            ]
            .groupby(
                ["POSSPD", "PO", "Servicio", "Sentido", "Periodo", "Día"],
                as_index=False,
            )
            .agg({"SumaExpediciones": "sum", "PromVelocidad": "mean"})
        )

    else:
        df.rename(
            columns={
                "media_hora": "Periodo",
                "Numero de Periodo": "Correlativo Periodo",
                "N° Salidas": "SumaExpediciones",
                "velocidad": "PromVelocidad",
                "id_servicio": "Servicio",
            },
            inplace=True,
        )
        return (
            df[
                [
                    "POSSPD",
                    "PO",
                    "Servicio",
                    "Sentido",
                    "Periodo",
                    "Día",
                    "SumaExpediciones",
                    "PromVelocidad",
                    "Correlativo Periodo",
                ]
            ]
            .groupby(
                ["POSSPD", "PO", "Servicio", "Sentido", "Periodo", "Día"],
                as_index=False,
            )
            .agg({"SumaExpediciones": "sum", "PromVelocidad": "mean"})
        )


def transformar_lbs(archivo_lbs: str, diccionario: str):
    """Transforma LBS para cruzar con eventos exogenos para Unidades de Servicio (Lic. 2019).

    :param archivo_lbs: Ruta del archivo LBS.
    :type :archivo_lbs: str

    :param diccionario: Ruta del archivo de diccionario de periodos.
    :type diccionario: str

    :return: LBS transformado.
    :rtype: pandas.core.frame.DataFrame
    """
    df = pd.read_csv(
        archivo_lbs,
        usecols=[
            "Servicio",
            "Sentido",
            "Fecha",
            "Tipo Día",
            "Cada media hora",
            "Distancia (m)",
            "Tiempo (hh:mm:ss)",
        ],
        dtype={
            "Servicio": str,
            "Tiempo (hh:mm:ss)": str,
            "Cada media hora": str,
            "Fecha": str,
        },
    )
    df = df.loc[~df["Tiempo (hh:mm:ss)"].isna()]
    df["Tiempo (hh:mm:ss)"] = df["Tiempo (hh:mm:ss)"].apply(lambda x: make_delta(x))
    df.rename(columns={"Cada media hora": "Periodo"}, inplace=True)

    df["SumaTiempo"] = df.groupby(
        ["Servicio", "Sentido", "Fecha", "Periodo", "Tipo Día"]
    )["Tiempo (hh:mm:ss)"].transform("sum")
    df["SumaDistancia"] = df.groupby(
        ["Servicio", "Sentido", "Fecha", "Periodo", "Tipo Día"]
    )["Distancia (m)"].transform("sum")
    df_conteo = (
        df.value_counts(["Servicio", "Sentido", "Fecha", "Periodo", "Tipo Día"])
        .reset_index()
        .rename(columns={"count": "SumaExpediciones"})
    )
    df["SumaDistancia"] = df["SumaDistancia"] / 1000
    df["SumaTiempo"] = df["SumaTiempo"].dt.total_seconds() / (60 * 60)
    df["PromVelocidad"] = df["SumaDistancia"] / df["SumaTiempo"]
    df = df.merge(
        df_conteo,
        on=["Servicio", "Sentido", "Fecha", "Periodo", "Tipo Día"],
        how="left",
    )

    df.drop(columns=["Distancia (m)", "Tiempo (hh:mm:ss)"], inplace=True)
    df = df.drop_duplicates()

    df_dic = pd.read_excel(diccionario, dtype={"Hora inicio": str})
    df = df.merge(
        df_dic,
        left_on=["Periodo", "Tipo Día"],
        right_on=["Hora inicio", "Tipo Día"],
        how="left",
    )

    df["Fecha"] = df["Fecha"].apply(lambda x: a_datetime(x))
    df["Fecha"] = pd.to_datetime(df["Fecha"], format="%d/%m/%Y")
    df["Fecha"] = df["Fecha"].apply(lambda x: excel_date(x))
    df["Sentido"] = df["Sentido"].map({"Ida": "I", "Regreso": "R", "Ret": "R"})
    df["SSPD"] = (
        df["Servicio"].apply(lambda x: str(x))
        + "-"
        + df["Sentido"]
        + "-"
        + df["Numero de Periodo"]
        + "-"
        + df["Fecha"].apply(lambda x: str(x))
    )

    df = df.groupby(
        ["SSPD", "Servicio", "Sentido", "Periodo", "Fecha", "Numero de Periodo"],
        as_index=False,
    ).agg(
        SumaExpediciones=("SumaExpediciones", "sum"),
        PromVelocidad=("PromVelocidad", "mean"),
        SumaDistancia=("SumaDistancia", "sum"),
        SumaTiempo=("SumaTiempo", "sum"),
    )

    return df[
        [
            "SSPD",
            "Servicio",
            "Sentido",
            "Periodo",
            "Fecha",
            "SumaExpediciones",
            "PromVelocidad",
            "SumaDistancia",
            "SumaTiempo",
            "Numero de Periodo",
        ]
    ]


def transformar_lbs_un(
    archivo_lbs: str, diccionario_periodos: str, dir_diccionario_codigos: str = None
):
    """Transforma LBS para cruzar con eventos exogenos para Unidades de negocios (Lic. 2012).

    :param archivo_lbs: Ruta del archivo LBS.
    :type :archivo_lbs: str

    :param diccionario_periodos: Ruta del archivo de diccionario de periodos.
    :type diccionario: str

    :param dir_diccionario_codigos: Ruta del archivo de diccionario de codigos.
    :type dir_diccionario_codigos: str[opcional]

    :return: LBS transformado.
    :rtype: pandas.core.frame.DataFrame
    """
    df = pd.read_csv(
        archivo_lbs,
        usecols=[
            "Servicio",
            "Sentido",
            "Fecha",
            "Tipo Día",
            "Cada media hora",
            "Distancia (m)",
            "Tiempo (hh:mm:ss)",
        ],
        dtype={
            "Servicio": str,
            "Tiempo (hh:mm:ss)": str,
            "Cada media hora": str,
            "Fecha": str,
        },
    )
    df["Tiempo (hh:mm:ss)"] = df["Tiempo (hh:mm:ss)"].apply(lambda x: make_delta(x))
    df.rename(columns={"Cada media hora": "Periodo"}, inplace=True)

    if dir_diccionario_codigos != None:
        df = df.merge(
            pd.read_excel(
                dir_diccionario_codigos, dtype={"cod_ts": str, "cod_sig": str}
            ),
            left_on=["Servicio"],
            right_on=["cod_sig"],
            how="left",
        )
        df.drop(columns=["Servicio"], inplace=True)
        df.rename(columns={"cod_ts": "Servicio"}, inplace=True)

    df["SumaTiempo"] = df.groupby(
        ["Servicio", "Sentido", "Fecha", "Periodo", "Tipo Día"]
    )["Tiempo (hh:mm:ss)"].transform("sum")
    df["SumaDistancia"] = df.groupby(
        ["Servicio", "Sentido", "Fecha", "Periodo", "Tipo Día"]
    )["Distancia (m)"].transform("sum")
    df_conteo = (
        df.value_counts(["Servicio", "Sentido", "Fecha", "Periodo", "Tipo Día"])
        .reset_index()
        .rename(columns={"count": "SumaExpediciones"})
    )
    df["SumaDistancia"] = df["SumaDistancia"] / 1000
    df["SumaTiempo"] = df["SumaTiempo"].dt.total_seconds() / (60 * 60)
    df["PromVelocidad"] = df["SumaDistancia"] / df["SumaTiempo"]
    df = df.merge(
        df_conteo,
        on=["Servicio", "Sentido", "Fecha", "Periodo", "Tipo Día"],
        how="left",
    )

    df.drop(columns=["Distancia (m)", "Tiempo (hh:mm:ss)"], inplace=True)
    df = df.drop_duplicates()

    df_dic = pd.read_excel(diccionario_periodos, dtype={"Hora inicio": str})
    df = df.merge(
        df_dic,
        left_on=["Periodo", "Tipo Día"],
        right_on=["Hora inicio", "Tipo Día"],
        how="left",
    )

    df["Fecha"] = df["Fecha"].apply(lambda x: a_datetime(x))
    df["Fecha"] = pd.to_datetime(df["Fecha"], format="%d/%m/%Y")
    df["Fecha"] = df["Fecha"].apply(lambda x: excel_date(x))
    df["Sentido"] = df["Sentido"].map({"Ida": "I", "Regreso": "R", "Ret": "R"})
    df["SSPD"] = (
        df["Servicio"].apply(lambda x: str(x))
        + "-"
        + df["Sentido"]
        + "-"
        + df["Numero de Periodo"].apply(lambda x: str(x))
        + "-"
        + df["Fecha"].apply(lambda x: str(x))
    )

    df = df.groupby(
        ["SSPD", "Servicio", "Sentido", "Fecha", "Numero de Periodo"], as_index=False
    ).agg(
        SumaExpediciones=("SumaExpediciones", "sum"),
        PromVelocidad=("PromVelocidad", "mean"),
        SumaDistancia=("SumaDistancia", "sum"),
        SumaTiempo=("SumaTiempo", "sum"),
    )

    return df[
        [
            "SSPD",
            "Servicio",
            "Sentido",
            "Numero de Periodo",
            "Fecha",
            "SumaExpediciones",
            "PromVelocidad",
            "SumaDistancia",
            "SumaTiempo",
        ]
    ]
