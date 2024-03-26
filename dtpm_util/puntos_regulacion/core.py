import os
from datetime import datetime, time

import pandas as pd
from dtpm_util.puntos_regulacion.funciones import (
    carga_patentes_seremitt,
    contar_buses_por_instante,
    determinar_un,
    procesamiento_registros_pr,
)


def calculo_demoras_conteo_buses_por_pr(
    dir_datos: str,
    nombre_archivos_pr: str,
    nombre_archivos_patentes: str,
    dir_listado_pr: str,
    dir_resultados: str = "resultados",
    tiempo_regulando = "00:10:00",
    frecuencia: str = "15min"
):
    """Función que calcula las demoras de los buses en cada PR

    param: dir_datos: Ruta donde se encuentran los datos de entrada
    type: dir_datos: str

    param: nombre_archivos_pr: Nombre tipo de los archivos de pulsaciones de PR
    type: nombre_archivos_pr: str

    param: nombre_archivos_patentes: Nombre tipo de los archivos de las patentes habilitadas por dia
    type: nombre_archivos_patentes: str

    param: dir_listado_pr: Ruta donde se encuentra el archivo con el listado de los PR
    type: dir_listado_pr: str

    param: dir_resultados: Ruta donde se guardan los resultados
    type: dir_resultados: str [Opcional]

    param: frecuencia: Frecuencia de tiempo de medición
    type: frecuencia: str [Opcional]   

    :return: Dos arcivos en la ruta "~/dir_resultados": "Conteos_Buses_PR.csv" y "Demoras_Buses_PR.csv"
    """

    start = datetime.now()
    print(
        "\n Fecha y hora de inicio de la corrida del script:",
        start.strftime("%d-%m-%Y %H:%M:%S"),
    )
    listado_de_pr = pd.read_excel(dir_listado_pr, engine="openpyxl")
    df_servicios = pd.read_excel(
        "datos/servicios.xlsx", dtype={"id_ss": str, "id_servicio": str}
    )
    # -- # -- # 1. PROCESAMIENTYO Y CARGA DE DATOS # -- # -- #

    # ------ 1.1 PROCESAMIENTYO PULSACIONES ------------------
    if not os.path.exists("checkpoints/"):
        os.makedirs("checkpoints/")
    archivos_cargados = os.listdir("checkpoints/")

    PR_preliminar = procesamiento_registros_pr(
        dir_datos, nombre_archivos_pr, listado_de_pr, archivos_cargados, tiempo_regulando= tiempo_regulando
    )
    PR_preliminar = PR_preliminar.drop_duplicates()
    # ------ 1.2 CARGA PATENTES POR DIA DEL SEREMITT ---------
    print("Cargando patentes habilitadas por día.... \n ")
    PPU_seremitt = carga_patentes_seremitt(dir_datos, nombre_archivos_patentes)

    # -- # -- # 2. PRIMER PRODUCTO: DEMORAS DE BUSES POR PUNTO DE REGULACIÓN # -- # -- #
    bd_buses_pr_demoras = determinar_un(PR_preliminar, PPU_seremitt)
    bd_buses_pr_demoras["SS Salida PR"] = bd_buses_pr_demoras["SS Salida PR"].fillna(
        "No Asignado"
    )

    bd_buses_pr_demoras = bd_buses_pr_demoras.merge(
        df_servicios, how="left", left_on="SS Salida PR", right_on="id_ss"
    )

    # bd_buses_pr_demoras = bd_buses_pr_demoras.drop_duplicates()
    bd_buses_pr_demoras.drop(columns=["id_ss"], inplace=True)
    print(
        "Se ha generado la tabla de tiempos de demora de buses por cada punto de regulación."
    )
    if not os.path.exists(dir_resultados):
        os.makedirs(dir_resultados)
    archivo_salida_01 = "Demoras_Buses_PR.csv"
    bd_buses_pr_demoras.to_csv(
        os.path.join(dir_resultados, archivo_salida_01),
        sep=";",
        decimal=",",
        index=False,
    )

    # -- # -- # 3. SEGUNDO PRODUCTO: COMIENZO DEL CONTEO DE BUSES REGULANDO EN CADA PR # -- # -- # -- #
    salida_conteo_buses_un_pr = contar_buses_por_instante(
        bd_buses_pr_demoras, listado_de_pr, frecuencia=frecuencia
    )
    # salida_conteo_buses_un_pr = salida_conteo_buses_un_pr.merge(df_servicios, how='left', on='id_servicio')
    # salida_conteo_buses_un_pr.drop(columns=['id_ss'], inplace=True)
    print("Se ha generado la tabla de conteos de buses por cada punto de regulación.")
    archivo_salida_02 = "Conteos_Buses_PR.csv"
    salida_conteo_buses_un_pr.to_csv(
        os.path.join(dir_resultados, archivo_salida_02), sep=";", index=False
    )

    # -- # -- # -- # FIN SEGUNDO PRODUCTO # -- # -- # -- #

    end = datetime.now()
    print(
        "Fecha y hora de término de la corrida del script:",
        end.strftime("%d-%m-%Y %H:%M:%S"),
    )

    auxiliar_lapso = end - start
    lapso = auxiliar_lapso.total_seconds()

    hora_tiempo_lapso = int(lapso // 3600)
    minutos_tiempo_lapso = int((lapso % 3600) // 60)
    segundos_tiempo_lapso = int(lapso % 60)
    auxiliar_tiempo_lapso = time(
        hora_tiempo_lapso, minutos_tiempo_lapso, segundos_tiempo_lapso
    )
    lapso_hh_mm_ss = auxiliar_tiempo_lapso.strftime("%H:%M:%S")

    print("El tiempo transcurrido de ejecución del script es de", lapso, "segundos.")
    print("Equivalentemente:", lapso_hh_mm_ss)
