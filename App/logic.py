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
        print(ar.get_element(lp.get(catalog, "load_time"), 6))

        return generales, first_five, last_five



# Funciones de consulta sobre el catálogo

def req_1(catalog, year):
    
    start_time = get_time()

    index = 0
    count = 0
    index_mayor = 0

    fecha_mayor = None


    for year_lista in lp.get(catalog, "year_collection")["elements"]:

        if year_lista == year:

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
