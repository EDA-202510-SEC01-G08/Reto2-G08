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


def req_5(catalog,categoria, year_i, year_f):
    """
    Retorna el resultado del requerimiento 5
"""
    # TODO: Modificar el requerimiento 5
    start_time = time.time()
    
    registros = ar.new_list()
    total_survey = 0
    total_census = 0
    total_registros = 0
    
    categorias = lp.get(catalog, "statical_category")["elements"]
    anios = lp.get(catalog, "year_collection")["elements"]
    fuentes = lp.get(catalog, "source")["elements"]
    tiempos_carga = lp.get(catalog, "load_time")["elements"]
    frecuencias = lp.get(catalog, "freq_collection")["elements"]
    estados = lp.get(catalog, "state_name")["elements"]
    unidades = lp.get(catalog, "unit_measurement")["elements"]
    productos = lp.get(catalog, "commodity")["elements"]
    
    for indice in catalog["statical_category"]["elements"]:
        categoria_actual = categorias[indice]
        anio_actual = int(anios[indice])
        
        if categoria_actual == categoria and year_i <= anio_actual <= year_f:
            fuente = fuentes[indice]
            fecha_carga = tiempos_carga[indice]
            frecuencia = frecuencias[indice]
            estado = estados[indice]
            unidad = unidades[indice]
            producto = productos[indice]
            
            mapa_registro = lp.new_map(3, 0.5)
            lp.put(mapa_registro, "load_time", fecha_carga)
            lp.put(mapa_registro, "state_name", estado)
            lp.put(mapa_registro, "index", indice)
            
            ar.add_last(registros, mapa_registro)
            total_registros += 1
            
            if fuente == "SURVEY":
                total_survey += 1
            elif fuente == "CENSUS":
                total_census += 1
    
    registros = ar.merge_sort(registros, sort_criteria_5)
    registros_finales = ar.new_list()
    
    for registro in registros["elements"]:
        idx = lp.get(registro, "index")
        fila = [fuentes[idx], anios[idx], tiempos_carga[idx], frecuencias[idx], estados[idx], unidades[idx], productos[idx]]
        ar.add_last(registros_finales, fila)
    
    end_time = time.time()
    execution_time = (end_time - start_time) * 1000
    
    return execution_time, total_registros, total_survey, total_census, registros_finales


def req_6(catalog):
    """
    Retorna el resultado del requerimiento 6
    """
    # TODO: Modificar el requerimiento 6
    pass


def req_7(catalog, departamento, anio_inicio, anio_fin, orden):
    """
    Retorna el resultado del requerimiento 7
    """
    # TODO: Modificar el requerimiento 7
    start_time = time.time()
    
    ingresos_por_anio = {}
    registros_validos = 0
    total_survey = 0
    total_census = 0
    registros_invalidos = 0
    
    anios = lp.get(catalog, "year_collection")["elements"]
    estados = lp.get(catalog, "state_name")["elements"]
    fuentes = lp.get(catalog, "source")["elements"]
    unidades = lp.get(catalog, "unit_measurement")["elements"]
    ingresos = lp.get(catalog, "income")["elements"]
    
    for i in range(len(anios)):
        if anio_inicio <= int(anios[i]) <= anio_fin and estados[i] == departamento and "$" in unidades[i]:
            es_valido = all(c.isdigit() or c in ['.', '-'] for c in ingresos[i])
            
            if es_valido:
                ingreso = float(ingresos[i])
                if anios[i] not in ingresos_por_anio:
                    ingresos_por_anio[anios[i]] = {"ingreso": 0, "registros": 0, "survey": 0, "census": 0}
                ingresos_por_anio[anios[i]]["ingreso"] += ingreso
                ingresos_por_anio[anios[i]]["registros"] += 1
                if fuentes[i] == "SURVEY":
                    ingresos_por_anio[anios[i]]["survey"] += 1
                    total_survey += 1
                elif fuentes[i] == "CENSUS":
                    ingresos_por_anio[anios[i]]["census"] += 1
                    total_census += 1
                registros_validos += 1
            else:
                registros_invalidos += 1
    
    ingresos_ordenados = sorted(ingresos_por_anio.items(), key=lambda x: (-x[1]["ingreso"], -x[1]["registros"])) if orden == "DESCENDENTE" else sorted(ingresos_por_anio.items(), key=lambda x: (x[1]["ingreso"], -x[1]["registros"]))
    
    if len(ingresos_ordenados) > 15:
        ingresos_ordenados = ingresos_ordenados[:5] + ingresos_ordenados[-5:]
    
    mayor_ingreso = ingresos_ordenados[0] if ingresos_ordenados else None
    menor_ingreso = ingresos_ordenados[-1] if ingresos_ordenados else None
    
    resultados = ar.new_list()
    for anio, datos in ingresos_ordenados:
        categoria = "MAYOR" if mayor_ingreso and datos["ingreso"] == mayor_ingreso[1]["ingreso"] else ""
        categoria += " MENOR" if menor_ingreso and datos["ingreso"] == menor_ingreso[1]["ingreso"] else ""
        ar.add_last(resultados, [anio, categoria.strip(), datos["ingreso"], datos["registros"], registros_invalidos, datos["survey"], datos["census"]])
    
    end_time = time.time()
    execution_time = (end_time - start_time) * 1000
    
    return execution_time, registros_validos, total_survey, total_census, resultados

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
