import time
import csv
from DataStructures.List import array_list as ar
from DataStructures.List import single_linked_list as sl
from DataStructures.Queue import queue as q
from DataStructures.Stack import stack as st
from DataStructures.Map import map_linear_probing as lp
from DataStructures.Map import map_separate_chaining as sc
import datetime as dt

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
"""
    # TODO: Modificar el requerimiento 5
    start_time = get_time()
    
    registros = ar.new_list()
    index= 0
    total_survey = 0
    total_census = 0
    total_registros = 0
    
    list_categorias = lp.get(catalog, "statical_category")
    list_anios = lp.get(catalog, "year_collection")
    list_fuentes = lp.get(catalog, "source")
    list_tiempos_carga = lp.get(catalog, "load_time")
    lits_frecuencias = lp.get(catalog, "freq_collection")
    lits_estados = lp.get(catalog, "state_name")
    lits_unidades = lp.get(catalog, "unit_measurement")
    lits_productos = lp.get(catalog, "commodity")
    
    for categoria in list_categorias["elements"]:
        categoria = categoria.replace(" ", "").upper()
        anio_actual = int(ar.get_element(list_anios,index)).replace(" ", "").upper()
        
        if categoria == category and year_i <= anio_actual <= year_f:
            load_time = dt.strptime(ar.get_element(list_tiempos_carga,index), "%Y-%m-%d %H:%M:%S")
            state = ar.get_element(lits_estados,index).replace(" ", "").upper()
            indice_record = index

            mapa_registro = lp.new_map(3, 0.5)
            lp.put(mapa_registro, "load_time", load_time)
            lp.put(mapa_registro, "state_name", state)
            lp.put(mapa_registro, "index", indice_record)  
            ar.add_last(registros, mapa_registro)
            total_registros += 1
            
            if ar.get_element(list_fuentes,index) == "SURVEY":
                total_survey += 1
            elif ar.get_element(list_fuentes,index) == "CENSUS":
                total_census += 1
    index += 1

    if total_registros == 0:
        return None
    
    registros_sorteados = ar.merge_sort(registros, sort_criteria_5)
    registros_finales = ar.new_list()

    for mapa in registros_sorteados["elements"]:
        ar.add_last(registros_finales, lp.get(mapa,"index"))

    resultados = ar.new_list()
    
    for i in registros_sorteados["elements"]:
        idx = lp.get(i, "index")
        fila = [ar.get_element(list_fuentes,idx),
                 ar.get_element(list_anios,idx), 
                 ar.get_element(list_tiempos_carga,idx), 
                 ar.get_element(lits_frecuencias,idx), 
                 ar.get_element(lits_estados,idx), 
                 ar.get_element(lits_unidades,idx), 
                 ar.get_element(lits_productos,idx)]
        ar.add_last(resultados, fila)
    
    end_time = get_time()
    delta = str(round(delta_time(start_time, end_time) ,2)) + " ms"

    general = ar.new_list()
    ar.add_last(general, delta)
    ar.add_last(general, total_registros)  
    ar.add_last(general, total_survey)
    ar.add_last(general, total_census) 

    
    return general, resultados

def sort_criteria_5(record1, record2):

    load_time1 = lp.get(record1, "load_time")
    load_time2 = lp.get(record2, "load_time")
    state_name1 = lp.get(record1, "state_name")
    state_name2 = lp.get(record2, "state_name")

    if load_time1 < load_time2:
        return True
    elif load_time1 == load_time2:
        return state_name1 < state_name2
    else:
        return False


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
    """"start_time = get_time()
    
    registros = ar.new_list()

    mapa_years = lp.new_map(anio_fin-anio_inicio, 0.5)

    index= 0
    total_survey = 0
    total_census = 0
    registros_validos = 0
    registros_invalidos = 0
    
    list_categorias = lp.get(catalog, "statical_category")
    list_anios = lp.get(catalog, "year_collection")
    list_fuentes = lp.get(catalog, "source")
    list_tiempos_carga = lp.get(catalog, "load_time")
    lits_frecuencias = lp.get(catalog, "freq_collection")
    lits_estados = lp.get(catalog, "state_name")
    lits_unidades = lp.get(catalog, "unit_measurement")
    lits_productos = lp.get(catalog, "commodity")
    
    for estado in lits_estados["elements"]:
        estado = estado.replace(" ", "").upper()
        anio_actual = int(ar.get_element(list_anios,index)).replace(" ", "").upper()
        unidad = ar.get_element(lits_unidades,index).replace(" ", "")
        valor = ar.get_element(lits_productos,index).replace(" ", "")

        if estado == departamento and anio_inicio <= anio_actual <= anio_fin and "$" in unidad:
            if lp.contains(mapa_years, anio_actual) is False:
                lp.put(mapa_years, anio_actual, 0)
            lp.get(mapa_years, anio_actual) += valor 
            registros_validos += 1
        if "(" in unidad:
            registros_invalidos +=1
        elif ar.get_element(list_fuentes,index) == "SURVEY":
            total_survey += 1
        elif ar.get_element(list_fuentes,index) == "CENSUS":
            total_census += 1

            """""

    """
    Retorna el resultado del requerimiento 7.
    Filtra los registros por departamento y rango de años, y los ordena por ingresos.
    """

    # Iniciar conteo de tiempo de ejecución
    start_time = get_time()

    # Crear un mapa para almacenar la información de ingresos por año
    mapa_years = lp.new_map(anio_fin - anio_inicio + 1, 0.5)

    # Contadores para estadísticas finales
    registros_validos = 0
    registros_invalidos = 0
    total_survey = 0
    total_census = 0

    # Obtener listas del catálogo
    list_anios = lp.get(catalog, "year_collection")
    list_fuentes = lp.get(catalog, "source")
    list_estados = lp.get(catalog, "state_name")
    list_unidades = lp.get(catalog, "unit_measurement")
    list_valores = lp.get(catalog, "value")

    # Recorrer la lista de estados para filtrar los registros
    for i in range(ar.size(list_estados)):
        estado = ar.get_element(list_estados, i)  # Obtener el nombre del estado
        anio_actual = ar.get_element(list_anios, i)  # Obtener el año de recolección
        unidad = ar.get_element(list_unidades, i)  # Obtener la unidad de medida
        valor = ar.get_element(list_valores, i)  # Obtener el valor de ingreso

        # Convertir el año a entero para comparaciones
        anio_actual = int(anio_actual)

        # Filtrar los registros según el departamento, año y unidad de medida en dólares
        if estado == departamento and anio_inicio <= anio_actual <= anio_fin and "$" in unidad:
            
            # Verificar si el valor de ingreso es válido (descartar si tiene caracteres como '()')
            if "(" in valor or valor == "":
                registros_invalidos += 1
            else:
                # Si el año no está en el mapa, se inicializa
                if not lp.contains(mapa_years, anio_actual):
                    datos_anio = lp.new_map(2, 0.5)
                    lp.put(datos_anio, "value", 0)  # Inicializar ingreso en 0
                    lp.put(datos_anio, "num_registros", 0)  # Inicializar cantidad de registros en 0
                    lp.put(mapa_years, anio_actual, datos_anio)  # Agregar al mapa principal
                
                # Obtener el mapa de datos del año actual
                datos_anio = lp.get(mapa_years, anio_actual)

                # Obtener los valores actuales de ingresos y cantidad de registros
                ingreso_actual = lp.get(datos_anio, "value")
                cantidad_actual = lp.get(datos_anio, "num_registros")

                # Sumar el nuevo valor de ingreso y aumentar el contador de registros
                lp.put(datos_anio, "value", ingreso_actual + float(valor))
                lp.put(datos_anio, "num_registros", cantidad_actual + 1)

                registros_validos += 1  # Aumentar el contador de registros válidos

        # Contabilizar registros según la fuente de origen
        fuente = ar.get_element(list_fuentes, i)
        if fuente == "SURVEY":
            total_survey += 1
        elif fuente == "CENSUS":
            total_census += 1

    # Aplicar la función de ordenamiento usando merge sort
    sorted_years = ar.merge_sort(mapa_years, sort_criteria_7)

    # Si el usuario quiere en orden ascendente, se invierte la lista
    # Se infiere que si es "DESCENDENTE" no se invierte sino se deja normal
    if orden == "ASCENDENTE":
        sorted_years = sorted_years[::-1]

    # Determinar los años con mayor y menor ingreso según el orden solicitado
    if ar.size(sorted_years) > 0:
        if orden == "ASCENDENTE":
            menor_ingreso = ar.get_element(sorted_years, 0)  # Primer año en la lista
            mayor_ingreso = ar.get_element(sorted_years, ar.size(sorted_years) - 1)  # Último año
        else:
            mayor_ingreso = ar.get_element(sorted_years, 0)  # Primer año en la lista
            menor_ingreso = ar.get_element(sorted_years, ar.size(sorted_years) - 1)  # Último año
    else:
        mayor_ingreso = None
        menor_ingreso = None

    # Medir el tiempo de ejecución
    end_time = get_time()
    execution_time = delta_time(start_time, end_time)

    # Retornar los resultados en orden
    # el return no se como hacerlo con listas tocaria cambiar algunas cosas
    return execution_time,registros_validos ,registros_invalidos, sorted_years, mayor_ingreso, menor_ingreso, registros_validos, registros_invalidos, total_survey, total_census


def sort_criteria_7(anio1, anio2):
    """
    Compara dos años para ordenarlos primero por ingresos y, en caso de empate,
    por el número de registros de manera descendente.
    """

    # Obtener los datos de los años del mapa `mapa_years`  
    datos_anio1 = lp.get(mapa_years, anio1) #no se como meter el mapa principal aqui
    datos_anio2 = lp.get(mapa_years, anio2)

    # Acceder a los valores de "value" (ingresos) y "num_registros" (número de registros)
    ingresos1 = lp.get(datos_anio1, "value")
    ingresos2 = lp.get(datos_anio2, "value")
    registros1 = lp.get(datos_anio1, "num_registros")
    registros2 = lp.get(datos_anio2, "num_registros")

    # Comparar por ingresos
    if ingresos1 < ingresos2:
        return True
    elif ingresos1 == ingresos2:
        # Si los ingresos son iguales, ordenar por número de registros (de manera descendente)
        return registros1 > registros2
    else:
        return False
    
        
        


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
