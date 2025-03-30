import time
import csv
from DataStructures.List import array_list as ar
from DataStructures.List import single_linked_list as sl
from DataStructures.Queue import queue as q
from DataStructures.Stack import stack as st
from DataStructures.Map import map_linear_probing as lp
from DataStructures.Map import map_separate_chaining as sc

csv.field_size_limit(2147483647)

def new_logic():
    """
    Crea el catalogo para almacenar las estructuras de datos
    """
    #TODO: Llama a las funciónes de creación de las estructuras de datos 
    #HECHO
    catalog = lp.new_map(11, 0.5, 109345121)

    lp.put(catalog, "source", ar.new_list())
    lp.put(catalog, "commodity", ar.new_list())
    lp.put(catalog, "statical_category", ar.new_list())
    lp.put(catalog, "unit_measurement", ar.new_list())
    lp.put(catalog, "state_name", ar.new_list())
    lp.put(catalog, "location", ar.new_list())
    lp.put(catalog, "year_collection", ar.new_list())
    lp.put(catalog, "freq_collection", ar.new_list())
    lp.put(catalog, "reference_period", ar.new_list())
    lp.put(catalog, "load_time", ar.new_list())
    lp.put(catalog, "value", ar.new_list())

    return catalog


# Funciones para la carga de datos

def load_data(catalog, filename):
    """
    Carga los datos del reto
    """
    # TODO: Realizar la carga de datos
    with open(filename, mode = "r", encoding='utf-8') as file:
        start_time = get_time()
        input_file = csv.DictReader(file)
        count = 0
        año_max = 0
        año_min = 0
        last_five = sl.new_list()
        first_five = sl.new_list()

        for x in input_file:
            ar.add_last(lp.get(catalog, "source"), x["source"])
            ar.add_last(lp.get(catalog, "commodity"), x["commodity"])
            ar.add_last(lp.get(catalog, "statical_category"), x["statical_category"])
            ar.add_last(lp.get(catalog, "unit_measurement"), x["unit_measurement"])
            ar.add_last(lp.get(catalog, "state_name"), x["state_name"])
            ar.add_last(lp.get(catalog, "location"), x["location"])
            ar.add_last(lp.get(catalog, "year_collection"), x["year_collection"])
            ar.add_last(lp.get(catalog, "freq_collection"), x["freq_collection"])
            ar.add_last(lp.get(catalog, "reference_period"), x["reference_period"])
            ar.add_last(lp.get(catalog, "load_time"), x["load_time"])
            ar.add_last(lp.get(catalog, "value"), x["value"])

            if int(x["year_collection"]) > año_max or año_max == 0:
                año_max = int(x["year_collection"])
            if int(x["year_collection"]) < año_min or año_min == 0:
                año_min = int(x["year_collection"])
            count += 1

            datos_listas = {"year_collection": x["year_collection"],
                            "load_time": x["load_time"],
                            "state_name": x["state_name"],
                            "source": x["source"],
                            "unit_measurement": x["unit_measurement"],
                            "value": x["value"]}
            if count < 5:
                sl.add_last(first_five, datos_listas)

            elif count >= 5:
                sl.add_last(last_five, datos_listas)
                if sl.size(last_five) > 5:
                    sl.remove_first(first_five)
        end_time = get_time()
        delta = delta_time(start_time, end_time)
        return delta, count, año_min, año_max, last_five, first_five

    

# Funciones de consulta sobre el catálogo

def get_data(catalog, id):
    """
    Retorna un dato por su ID.
    """
    #TODO: Consulta en las Llamar la función del modelo para obtener un dato
    pass


def req_1(catalog):
    """
    Retorna el resultado del requerimiento 1
    """
    # TODO: Modificar el requerimiento 1
    pass


def req_2(catalog):
    """
    Retorna el resultado del requerimiento 2
    """
    # TODO: Modificar el requerimiento 2
    pass


def req_3(catalog):
    """
    Retorna el resultado del requerimiento 3
    """
    # TODO: Modificar el requerimiento 3
    pass


def req_4(catalog):
    """
    Retorna el resultado del requerimiento 4
    """
    # TODO: Modificar el requerimiento 4
    pass


def req_5(catalog,category, year_i, year_f):
    """
    Retorna el resultado del requerimiento 5
    Como analista de datos agrícola quiero consultar los registros recopilados para una categoría estadística de mi 
interés entre un rango de años de recopilación dado. 
Los parámetros de entrada de este requerimiento son: 
• Categoría estadística para filtrar (ej.: “INVENTORY”, “SALES”, etc.) 
• Año inicial del periodo a consultar (con formato "YYYY ", ej.: “2007”). 
• Año final del periodo a consultar (con formato "YYYY", ej.: “2010”). 
La respuesta esperada debe contener: 
• Tiempo de la ejecución del requerimiento en milisegundos. 
• Número total de registros que cumplieron el filtro. 
• Número total de registros con tipo de fuente/origen “SURVEY” 
• Número total de registros con tipo de fuente/origen “CENSUS” 
• Para el listado de registros resultante, presentar por cada uno de los registros la siguiente información 
ordenados de manera descendente por fecha de carga del registro: 
o Tipo de fuente/origen del registro. (ej.: “CENSUS” o “SURVEY”) 
o Año de recopilación del registro. (ej.: “2007”) 
o Fecha de carga del registro. (con formato "%Y-%m-%d". ej.: “2012-05-15”). 
o Frecuencia de la recopilación del registro. (ej.: “ANNUAL”, “WEEKLY”, etc.) 
o Nombre del departamento del registro.  
o Unidad de medición del registro. (ej.: “HEAD”, “$”, etc.) 
o Tipo del producto del registro (ej.: “HOGS”, “SHEEP”, etc.) 
    """
    # TODO: Modificar el requerimiento 5
    start_time = get_time()

    load_time_map = lp.new_map(ar.size(lp.get(catalog, "load_time")), 0.5)

    index = 0
    count = 0
    count_survey = 0
    count_census = 0

    for categoria in lp.get(catalog, "statical_category")["elements"]:
        categoria = categoria.replace(" ", "").upper()

        if categoria == category:
            year = int(ar.get_element(lp.get(catalog, "year_collection"), index).replace(" ", ""))
            if year_i <= year <= year_f:
                load_time = dt.strptime(ar.get_element(lp.get(catalog, "load_time"), index), "%m/%d/%Y %H:%M")
                if lp.contains(load_time_map, load_time) is False:
                    lp.put(load_time_map, load_time, ar.new_list())
                ar.add_last(lp.get(load_time_map, load_time), index)

                count += 1

                if ar.get_element(lp.get(catalog, "source"), index).replace(" ", "").upper() == "SURVEY":
                    count_survey += 1

                elif ar.get_element(lp.get(catalog, "source"), index).replace(" ", "").upper() == "CENSUS":
                    count_census += 1
        index += 1

    if count == 0:
        return None

    load_times = lp.key_set(load_time_map)
    sorted_load_times = ar.merge_sort(load_times, ar.default_sort_criteria)

    sorted_indices = []
    for load_time in sorted_load_times["elements"]:
        indices = lp.get(load_time_map, load_time)
        sorted_indices_i = ar.merge_sort_indice(indices, ar.sort_states_indices, catalog)
        sorted_indices.extend(sorted_indices_i["elements"])

    result = ar.new_list()
    for idx in sorted_indices:
        record = []
        record.append(ar.get_element(lp.get(catalog, "source"), idx))
        record.append(ar.get_element(lp.get(catalog, "year_collection"), idx))
        record.append(ar.get_element(lp.get(catalog, "load_time"), idx))
        record.append(ar.get_element(lp.get(catalog, "freq_collection"), idx))
        record.append(ar.get_element(lp.get(catalog, "state_name"), idx))
        record.append(ar.get_element(lp.get(catalog, "unit_measurement"), idx))
        record.append(ar.get_element(lp.get(catalog, "commodity"), idx))

        ar.add_last(result, record)

    end_time = get_time()
    delta = str(round(delta_time(start_time, end_time), 2)) + " ms"

    general = ar.new_list()
    ar.add_last(general, delta)
    ar.add_last(general, count)
    ar.add_last(general, count_survey)
    ar.add_last(general, count_census)
#ar.add_last(general, count_survey + count_census) No uso esto pq no se si es cierto en todo caso

    return general, result


def req_6(catalog):
    """
    Retorna el resultado del requerimiento 6
    """
    # TODO: Modificar el requerimiento 6
    pass


def req_7(catalog):
    """
    Retorna el resultado del requerimiento 7
    """
    # TODO: Modificar el requerimiento 7
    pass


def req_8(catalog):
    """
    Retorna el resultado del requerimiento 8
    """
    # TODO: Modificar el requerimiento 8
    pass


# Funciones para medir tiempos de ejecucion

def get_time():
    """
    devuelve el instante tiempo de procesamiento en milisegundos
    """
    return float(time.perf_counter()*1000)


def delta_time(start, end):
    """
    devuelve la diferencia entre tiempos de procesamiento muestreados
    """
    elapsed = float(end - start)
    return elapsed
