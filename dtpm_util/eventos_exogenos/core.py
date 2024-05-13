import os
import pandas as pd
from dtpm_util.utilidades.preprocesamiento import concatenar_anexo_3
from dtpm_util.eventos_exogenos.preprocesamiento import transformar_anexo_3, transformar_lbs_un, transformar_lbs

def generar_insumos_eventos_exogenos_un(
    dir_anexo_3: str,
    dir_lbs: str,
    dir_diccionario_periodo: str,
    dir_diccionario_codigos_servicios: str,
    dir_resultados: str = "resultados_eventos_exogenos",
):
    """Genera insumos para la revisión de eventos exógenos enviados por los operadores UN con el LBS y el anexo 3

    :param dir_anexo_3: Ruta de la carpeta de anexo 3 ordendas en carpetas por fecha.
    :type dir_anexo_3: str

    :param dir_lbs: Ruta de la carpeta con archivos de LBS.
    :type dir_lbs: str

    :param dir_diccionario_periodo: Ruta del archivo de Excel con diccionario de periodos LIC 2012.
    :type dir_diccionario_periodo: str

    :param dir_diccionario_codigos_servicios: Ruta del archivo de Excel con diccionario de codigos de servicios.
    :type dir_diccionario_codigos_servicios: str

    :param dir_resultados: Ruta de la carpeta donde se guardarán los archivos de resultados.
    :type dir_resultados: str [opcional]

    :return: Archivos de resultado en dir_resultados

    :Example:
    calculos.procesar_eventos_exogenos(
        dir_anexo_3='anexo_3',
        dir_lbs='lbs',
        dir_diccionario_periodo='diccionario_periodos.xlsx',
        dir_diccionario_codigos_servicios='diccionario_codigos_servicios.xlsx',
        dir_diccionario='diccionario.xlsx',
        dir_resultados='resultados'
    )

    """

    exist = os.path.exists(dir_resultados)
    if not exist:
        os.makedirs(dir_resultados)

    # -- # -- # -- # Concatenando y tansformando anexo 3 # -- # -- # -- #
    print("    Concatenando y Transformando anexos 3.......\n")
    anexos_tranformados = []
    for carpeta in [f for f in os.listdir(dir_anexo_3) if not os.path.isfile(f)]:
        fecha = carpeta.replace("_", "-")
        df = concatenar_anexo_3(
            os.path.join(dir_anexo_3, carpeta), dir_resultados, "", True
        )

        df = transformar_anexo_3(fecha, df, dir_diccionario_periodo, es_un=True)
        anexos_tranformados.append(df)
    df_anexo_3 = pd.concat(anexos_tranformados, axis=0)
    df_anexo_3.to_excel(os.path.join(dir_resultados, "anexo_3_transformado.xlsx"))

    # -- # -- # -- #  Concatenando y tansformando LBS # -- # -- # -- #
    print("\n    Concatenando y Transformando LBS.......\n")
    lbs_df = []

    archivos = os.listdir(dir_lbs)

    for archivo in archivos:
        lbs_df.append(pd.read_excel(os.path.join(dir_lbs, archivo)))
    df_lbs = pd.concat(lbs_df, axis=0)
    df_lbs.to_csv(os.path.join(dir_resultados, "lbs_merge.csv"), index=False)

    df = pd.read_csv(os.path.join(dir_resultados, "lbs_merge.csv"))
    df['check'] = df['Tiempo (hh:mm:ss)'].apply(lambda x : x if isinstance(x, float) else -1)
    df = df.loc[df['check'] == -1, :]
    df.drop(columns=['check'], inplace=True)
    df.to_csv(os.path.join(dir_resultados, "lbs_merge.csv"))

    print("\n    Transformando LBS.......\n")
    df_lbs = transformar_lbs_un(
        os.path.join(dir_resultados, "lbs_merge.csv"), dir_diccionario_periodo, dir_diccionario_codigos=dir_diccionario_codigos_servicios
    )
    df_lbs.to_excel(os.path.join(dir_resultados, "lbs_transformado.xlsx"), index=False)

def generar_insumos_eventos_exogenos(
    dir_anexo_3: str,
    dir_lbs: str,
    dir_diccionario_periodo: str,
    dir_diccionario_codigos_servicios: str,
    dir_resultados: str = "resultados_eventos_exogenos",
):
    """Genera insumos para la revisión de eventos exógenos enviados por los operadores US con el LBS y el anexo 3

    :param dir_anexo_3: Ruta de la carpeta de anexo 3 ordendas en carpetas por fecha.
    :type dir_anexo_3: str

    :param dir_lbs: Ruta de la carpeta con archivos de LBS.
    :type dir_lbs: str

    :param dir_diccionario_periodo: Ruta del archivo de Excel con diccionario de periodos LIC 2019.
    :type dir_diccionario_periodo: str

    :param dir_diccionario_codigos_servicios: Ruta del archivo de Excel con diccionario de codigos de servicios.
    :type dir_diccionario_codigos_servicios: str

    :param dir_resultados: Ruta de la carpeta donde se guardarán los archivos de resultados.
    :type dir_resultados: str [opcional]

    :return: Archivos de resultado en dir_resultados

    :Example:
    calculos.procesar_eventos_exogenos(
        dir_anexo_3='anexo_3',
        dir_lbs='lbs',
        dir_diccionario='diccionario.xlsx',
        dir_resultados='resultados'
    )

    """

    exist = os.path.exists(dir_resultados)
    if not exist:
        os.makedirs(dir_resultados)

    # -- # -- # -- # Concatenando y tansformando anexo 3 # -- # -- # -- #
    print("    Concatenando y Transformando anexos 3.......\n")
    anexos_tranformados = []
    for carpeta in [f for f in os.listdir(dir_anexo_3) if not os.path.isfile(f)]:
        fecha = carpeta.replace("_", "-")
        df = concatenar_anexo_3(
            os.path.join(dir_anexo_3, carpeta), dir_resultados, "", True
        )

        df = transformar_anexo_3(fecha, df, dir_diccionario_periodo, es_un=True)
        anexos_tranformados.append(df)
    df_anexo_3 = pd.concat(anexos_tranformados, axis=0)
    df_anexo_3.to_excel(os.path.join(dir_resultados, "anexo_3_transformado.xlsx"))

    # -- # -- # -- #  Concatenando y tansformando LBS # -- # -- # -- #
    print("\n    Concatenando y Transformando LBS.......\n")
    lbs_df = []

    archivos = os.listdir(dir_lbs)

    for archivo in archivos:
        lbs_df.append(pd.read_excel(os.path.join(dir_lbs, archivo)))
    df_lbs = pd.concat(lbs_df, axis=0)
    df_lbs.to_csv(os.path.join(dir_resultados, "lbs_merge.csv"), index=False)

    df = pd.read_csv(os.path.join(dir_resultados, "lbs_merge.csv"))
    df['check'] = df['Tiempo (hh:mm:ss)'].apply(lambda x : x if isinstance(x, float) else -1)
    df = df.loc[df['check'] == -1, :]
    df.drop(columns=['check'], inplace=True)
    df.to_csv(os.path.join(dir_resultados, "lbs_merge.csv"))

    print("\n    Transformando LBS.......\n")
    df_lbs = transformar_lbs(
        os.path.join(dir_resultados, "lbs_merge.csv"), dir_diccionario_periodo, dir_diccionario_codigos=dir_diccionario_codigos_servicios
    )
    df_lbs.to_excel(os.path.join(dir_resultados, "lbs_transformado.xlsx"), index=False)

    