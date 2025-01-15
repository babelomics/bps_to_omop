# 12/07/2024
#
# Ejemplo de uso para probar configuraciones para leer muchos csv
# Y crear un dict con instrucciones de lectura personalizadas.


# Cargamos paquetes que nos harán falta
from itertools import product

import pandas as pd
from utils.metadata import folders

import bps_to_omop.general as gen

# == Get the list of all relevant files ==
# Los archivos relevantes están en las carpetas
main_dir = folders["base"]
data_dirs = [
    f"{main_dir}/",
    f"{main_dir}/Segundo_dump/",
    f"{main_dir}/Farmacia/20230503/",
    f"{main_dir}/Farmacia/20230711/2020/",
    f"{main_dir}/Farmacia/20230711/2021/",
    f"{main_dir}/Farmacia/20230711/2022/",
    f"{main_dir}/Farmacia/20230711/2023/",
]
# Cribamos para archivos txt
file_list_test = []
for data_dir in data_dirs:
    file_list_test += gen.get_file_paths_on_cond(data_dir, end_str=".txt")


# == TRANSFORM ========================================================
# Candidatos para parametros que pueden variar entre archivos
candidate_params_test = {
    "sep": ["|", ";"],
    "skiprows": [0, 5],
}
# Parametros por defecto (deben funcionar para todos)
default_params_test = {"encoding": "latin9"}

funcs_to_check_test = [gen.is_first_col_NUHSA_like]

readoptions_dict_test = gen.get_reading_params(
    file_list_test, default_params_test, candidate_params_test, funcs_to_check_test
)

# Print results
print(readoptions_dict_test)
