import datetime

from xlrd import xldate

from dtpm_util.utilidades.constantes import CODIGOS


def to_decimal_time(time):
    """Transformar datetime a unidades de "dias".

    :param archivo_perfil: Tiempo (Fecha y hora).
    :type archivo_perfil: datetime.datetime

    :return: Tiempo en unidades de "dias".
    :rtype: float
    """
    horas = time.hour
    minutos = time.minute
    segundos = time.second

    return horas / 24 + minutos / 1440 + segundos / 86400


def to_fecha_hora(fila):
    """Crea datetime a partir de date y time de las columnas "Fecha" y "Hora_Inicio_SIG" de un dataframe.

    :param fila: Fila de un dataframe (pandas.core.series.Series) con las columnas "Fecha" y "Hora_Inicio_SIG".
    :type fila: pandas.core.series.Series

    :return: Dato en formato datetime.datetime.
    :rtype: datetime.datetime
    """
    dia = fila["Fecha"].day
    mes = fila["Fecha"].month
    año = fila["Fecha"].year
    hora = fila["Hora_Inicio_SIG"].hour
    minu = fila["Hora_Inicio_SIG"].minute
    secs = fila["Hora_Inicio_SIG"].second
    fila["Inicio Sig"] = datetime.datetime(año, mes, dia, hora, minu, secs)
    hora = fila["Hora Inicio Viaje"].hour
    minu = fila["Hora Inicio Viaje"].minute
    secs = fila["Hora Inicio Viaje"].second
    fila["Hora Inicio Viaje"] = datetime.datetime(año, mes, dia, hora, minu, secs)
    return fila


def make_delta(entry:str):
    """Transforma de string a timedelta.

    :param entry: Texto con formato "hh:mm:ss".
    :type entry: str

    :return: Dato en formato datetime.timedelta.
    :rtype: datetime.timedelta
    """

    h, m, s = entry.split(":")
    return datetime.timedelta(hours=int(h), minutes=int(m), seconds=int(s))


def excel_date(date):
    """Transforma de datetime a formato 'date de Excel'.

    :param date: Fecha en formato datetime.datetime.
    :type date: datetime.datetime

    :return: Fecha en formato 'date de Excel'.
    :rtype: xldate
    """

    return int(
        xldate.xldate_from_date_tuple(
            date_tuple=(date.year, date.month, date.day), datemode=0
        )
    )


def a_datetime(x: str):
    """Transforma de string a datetime.

    :param x: Texto con formato "%d-%m-%Y" o "%d/%m/%Y".
    :type x: str

    :return: Dato en formato datetime.datetime.
    :rtype: datetime.datetime

    """

    try:
        return datetime.datetime.strptime(x, "%d-%m-%Y")
    except:
        return datetime.datetime.strptime(x, "%d/%m/%Y")


def to_unidad(x: str, dict_servicios: dict) -> str:
    """Transforma de codigo TS a unidad de sevicio/negocio.

    :param x: Codigo TS.
    :type x: str

    :return: Unidad de servicio/negocio.
    :rtype: str

    """

    return dict_servicios[str(x)]["id_unidad"]

def to_cod_usuario(x: str, dict_servicios: dict) -> str:
    """Transforma de codigo TS a codigo de usuario.

    :param x: Codigo TS.
    :type x: str

    :return: Codigo Usuario.
    :rtype: str

    """

    return dict_servicios[str(x)]["id_unidad"]
