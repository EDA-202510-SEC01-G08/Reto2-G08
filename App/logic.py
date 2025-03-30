import time
import csv
from DataStructures.List import array_list as ar
from DataStructures.List import single_linked_list as sl
from DataStructures.Queue import queue as q
from DataStructures.Stack import stack as st
from DataStructures.Map import map_linear_probing as lp
from DataStructures.Map import map_separate_chaining as sc
from datetime import datetime as dt

csv.field_size_limit(2147483647)

def new_logic():
    """
    Crea el catalogo para almacenar las estructuras de datos
    """
    #TODO: Llama a las funciónes de creación de las estructuras de datos 
    # HECHO

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
    # HECHO
    with open(filename, mode = "r", encoding='utf-8') as file:
        start_time = get_time()
        input_file = csv.DictReader(file)
        count = 0
        año_max = 0
        año_min = 0
        last_five = ar.new_list()
        first_five = ar.new_list()

        for row in input_file:
            ar.add_last(lp.get(catalog, "source"), row["source"])
            ar.add_last(lp.get(catalog, "commodity"), row["commodity"])
            ar.add_last(lp.get(catalog, "statical_category"), row["statical_category"])
            ar.add_last(lp.get(catalog, "unit_measurement"), row["unit_measurement"])
            ar.add_last(lp.get(catalog, "state_name"), row["state_name"])
            ar.add_last(lp.get(catalog, "location"), row["location"])
            ar.add_last(lp.get(catalog, "year_collection"), row["year_collection"])
            ar.add_last(lp.get(catalog, "freq_collection"), row["freq_collection"])
            ar.add_last(lp.get(catalog, "reference_period"), row["reference_period"])
            ar.add_last(lp.get(catalog, "load_time"), row["load_time"])
            ar.add_last(lp.get(catalog, "value"), row["value"])

            if int(row["year_collection"]) > año_max or año_max == 0:
                año_max = int(row["year_collection"])
            if int(row["year_collection"]) < año_min or año_min == 0:
                año_min = int(row["year_collection"])

            datos_listas = [row["year_collection"],
                            row["load_time"],
                            row["state_name"],
                            row["source"],
                            row["unit_measurement"],
                            row["value"]]

            if count < 5:
                ar.add_last(first_five, datos_listas)

            elif count >= 5:
                ar.add_last(last_five, datos_listas)
                if ar.size(last_five) > 5:
                    ar.remove_first(last_five)

            count += 1

        end_time = get_time()
        delta = str(round(delta_time(start_time, end_time),2)) + " ms"

        generales = ar.new_list()
        ar.add_last(generales, delta)
        ar.add_last(generales, count)
        ar.add_last(generales, año_min)
        ar.add_last(generales, año_max)

        return generales, first_five, last_five



# Funciones de consulta sobre el catálogo

def req_1(catalog, year):
    
    start_time = get_time()

    index = 0
    count = 0
    index_mayor = 0

    fecha_mayor = None

    for year_lista in lp.get(catalog, "year_collection")["elements"]:
        int_year = int(year_lista.replace(" ", ""))

        if int_year == year:

            if fecha_mayor is None:

                fecha_mayor = ar.get_element(lp.get(catalog, "load_time"), index)
                fecha_mayor_dt = dt.strptime(fecha_mayor, "%m/%d/%Y %H:%M")
                index_mayor = index

            fecha_year = ar.get_element(lp.get(catalog, "load_time"), index)
            fecha_year_dt = dt.strptime(fecha_year, "%m/%d/%Y %H:%M")
            count += 1

            if fecha_year_dt >= fecha_mayor_dt: # por >= retorna el último
                fecha_mayor = ar.get_element(lp.get(catalog, "load_time"), index)
                fecha_mayor_dt = dt.strptime(fecha_mayor, "%m/%d/%Y %H:%M")
                index_mayor = index

        index += 1

    if fecha_mayor is None:
        return None 
    
    datos_mayor = ar.new_list()
    ar.add_last(datos_mayor,ar.get_element(lp.get(catalog, "year_collection"), index_mayor))
    ar.add_last(datos_mayor,ar.get_element(lp.get(catalog, "load_time"), index_mayor))
    ar.add_last(datos_mayor,ar.get_element(lp.get(catalog, "source"), index_mayor))
    ar.add_last(datos_mayor,ar.get_element(lp.get(catalog, "freq_collection"), index_mayor))
    ar.add_last(datos_mayor,ar.get_element(lp.get(catalog, "state_name"), index_mayor))
    ar.add_last(datos_mayor,ar.get_element(lp.get(catalog, "commodity"), index_mayor))
    ar.add_last(datos_mayor,ar.get_element(lp.get(catalog, "unit_measurement"), index_mayor))
    ar.add_last(datos_mayor,ar.get_element(lp.get(catalog, "value"), index_mayor))

    end_time = get_time()
    delta = str(round(delta_time(start_time, end_time),2)) + " ms"

    general = ar.new_list()
    ar.add_last(general, delta)
    ar.add_last(general, count)

    return general, datos_mayor

def req_2(catalog, state, N):
    start_time = get_time()
    mapa_tiempos = lp.new_map(ar.size(lp.get(catalog, "state_name")), 0.5) # q uso como size?????
    index = 0
    count = 0
    for estado in lp.get(catalog, "state_name")["elements"]:
        estadom = estado.replace(" ", "").upper()
        if estadom == state:
            load_time_date = dt.strptime(ar.get_element(lp.get(catalog, "load_time"), index),"%m/%d/%Y %H:%M")
            if lp.contains(mapa_tiempos, load_time_date) is False:
                lp.put(mapa_tiempos, load_time_date, ar.new_list())
            ar.add_last(lp.get(mapa_tiempos, load_time_date), index)
            count += 1
        index += 1
    


    if count == 0:
        return None
    
    tiempos = lp.key_set(mapa_tiempos)
    sorted_times = ar.merge_sort(tiempos, ar.default_sort_criteria)

    sorted_indices = []
    #sorted_indices = ar.new_list()

    for load_time in sorted_times["elements"]:
        indices = lp.get(mapa_tiempos, load_time)
        sorted_indices_i = ar.merge_sort_indice(indices, ar.sort_states_indices, catalog) # este resulta innecesario    
                                                                                  #considerando que los nombres de los estados son iguales
        sorted_indices.extend(sorted_indices_i["elements"])
        #for idx in sorted_indices_i["elements"]: #Que hago???????????
           # ar.add_last(sorted_indices, idx)

    result = ar.new_list()
    for idx in sorted_indices[-N:]:
        record = [] # puedo usar una lista de las creadas pero me toca volver a iterar en el view
        record.append(ar.get_element(lp.get(catalog, "year_collection"), idx))
        record.append(ar.get_element(lp.get(catalog, "load_time"), idx))
        record.append(ar.get_element(lp.get(catalog, "source"), idx))
        record.append(ar.get_element(lp.get(catalog, "freq_collection"), idx))
        record.append(ar.get_element(lp.get(catalog, "state_name"), idx))
        record.append(ar.get_element(lp.get(catalog, "commodity"), idx))
        record.append(ar.get_element(lp.get(catalog, "unit_measurement"), idx))
        record.append(ar.get_element(lp.get(catalog, "value"), idx))
        ar.add_last(result, record)

    end_time = get_time()
    delta = str(round(delta_time(start_time, end_time), 2)) + " ms"
    
    general = ar.new_list()
    ar.add_last(general, delta)
    ar.add_last(general, count)

    return general, result
                                        
def req_3(catalog):
    """
    Retorna el resultado del requerimiento 3
    """
    # TODO: Modificar el requerimiento 3
    pass


def req_4(catalog, prod, year_i, year_f):
    start_time = get_time()

    load_time_map = lp.new_map(ar.size(lp.get(catalog, "load_time")), 0.5)

    index = 0
    count = 0
    count_survey = 0
    count_census = 0

    for producto in lp.get(catalog, "commodity")["elements"]:
        producto = producto.replace(" ", "").upper()

        if producto == prod:
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

    
    


def req_5(catalog):
    """
    Retorna el resultado del requerimiento 5
    """
    # TODO: Modificar el requerimiento 5
    pass

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
