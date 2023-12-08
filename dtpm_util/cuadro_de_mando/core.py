import os
from dtpm_util.utilidades.preprocesamiento import (
    concatenar_anexo_3,
    concatenar_anexo_8,
)
from dtpm_util.cuadro_de_mando.operaciones import (
    codificar_perfil,
    generar_tabla_temp_1,
    generar_flota,
    generar_flota_mh,
    generar_ict,
    generar_icf,
    formato_flota,
    formato_ict,
    formato_icf,
)


def calcular(
    carpeta_anexos: str,
    archivo_perfil: str,
    dir_carpeta_resultados: str = "resultados_cuadro_de_mando",
    concatenar_anexos: bool = True,
):
    """Calcula según programa de operación los archivos necesarios para la carga del cuadro de mando.

    :param carpeta_anexos: Ruta de la carpeta con carpetas de anexo 3, anexo 4 (lic. 2019) y anexo 8 (lic. 2012).
    :type carpeta_anexos: str

    :param archivo_perfil: Ruta de la archivo con perfil teórico.
    :type archivo_perfil: str

    :param dir_carpeta_resultados: Ruta de la carpeta donde entregar los archivos de resultado.
    :type dir_carpeta_resultados: str [opcional]

    :param concatenar_anexos: Si es True, concatena los anexos 3 y 8 en un solo archivo, respectivamente.
    :type concatenar_anexos: bool [opcional]

    :return: Archivos de resultado en dir_carpeta_resultados.

    :Example:
    calculos.calculo_cuadro_de_mando(
        carpeta_anexos='anexos',
        archivo_perfil='perfil_teorico.xlsx',
        dir_carpeta_resultados='resultados'
    )

    """
    exist = os.path.exists(dir_carpeta_resultados)
    if not exist:
        os.makedirs(dir_carpeta_resultados)
    if concatenar_anexos:
        concatenar_anexo_3(
            carpeta_anexos, dir_carpeta_resultados, dir_subcarpeta="anexo_3"
        )
        concatenar_anexo_8(carpeta_anexos, dir_carpeta_resultados)

    print(
        "\n        # -- # -- # Generacion de archivos Cuadro de Mando # -- # -- # \n "
    )

    codificar_perfil(archivo_perfil, dir_carpeta_resultados)
    generar_tabla_temp_1(dir_carpeta_resultados)
    generar_flota(
        dir_carpeta_resultados,
        os.path.join(dir_carpeta_resultados, "temp_1.xlsx"),
    )
    generar_flota_mh(
        dir_carpeta_resultados, os.path.join(dir_carpeta_resultados, "flota.xlsx")
    )
    generar_ict(
        dir_carpeta_resultados,
        os.path.join(dir_carpeta_resultados, "flota.xlsx"),
        os.path.join(dir_carpeta_resultados, "perfil.xlsx"),
        os.path.join(dir_carpeta_resultados, "flota_mh.xlsx"),
    )
    generar_icf(dir_carpeta_resultados)
    formato_flota(
        dir_carpeta_resultados, os.path.join(dir_carpeta_resultados, "flota.xlsx")
    )
    formato_ict(
        dir_carpeta_resultados, os.path.join(dir_carpeta_resultados, "ict.xlsx")
    )
    formato_icf(
        dir_carpeta_resultados, os.path.join(dir_carpeta_resultados, "icf.xlsx")
    )

    print("        # -- # -- # Terminado!! # -- # -- #")
