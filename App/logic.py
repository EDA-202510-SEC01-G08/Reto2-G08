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


def req_3(catalog, año_i, año_f, departamento):
    """
    Retorna el resultado del requerimiento 3
    """
    # TODO: Modificar el requerimiento 3
    start_time = get_time()
    count_total = 0
    count_survey = 0
    count_census = 0
    lista_años_collección = lp.get(catalog, "year_collection")
    lista_departamento = lp.get(catalog, "state_name")
    lista_fuente = lp.get(catalog, "source")
    lista_frecuencia = lp.get(catalog, "freq_collection")
    lista_producto = lp.get(catalog, "commodity")
    lista_unidad = lp.get(catalog, "unit_measurement")
    lista_carga = lp.get(catalog, "load_time")
    size = ar.size(lista_años_collección)
    lista_datos = ar.new_list()

    for i in range(size):
        if ar.get_element(lista_años_collección, i) >= año_i and ar.get_element(lista_años_collección, i) <= año_f and ar.get_element(lista_departamento, i) == departamento:
            print("A VER SI ESTA COSA FUNCIONA")
            lista_un_dato = lp.new_map(2, 0.5)
            count_total += 1 
            if ar.get_element(lista_fuente, i) == "SURVEY":
                count_survey += 1
            elif ar.get_element(lista_fuente, i) == "CENSUS":
                count_census += 1

            lp.put(lista_un_dato, "load_time", ar.get_element(lista_carga, i))
            lp.put(lista_un_dato, "index", i)

            ar.add_last(lista_datos, lista_un_dato)

    if not ar.is_empty(lista_datos):
        lista_datos_sorted = ar.merge_sort(lista_datos, sort_criteria_3)
        index_sorted = ar.new_list()
        for j in lista_datos_sorted["elements"]:
            ar.add_last(index_sorted, lp.get(j, "index"))
        
        lista_final = ar.new_list()
        for z in index_sorted["elements"]:
            lista_un_dato = []
            lista_un_dato.append(ar.get_element(lista_fuente, z))
            lista_un_dato.append(ar.get_element(lista_años_collección, z))
            lista_un_dato.append(ar.get_element(lista_carga, z))
            lista_un_dato.append(ar.get_element(lista_frecuencia, z))
            lista_un_dato.append(ar.get_element(lista_producto, z))
            lista_un_dato.append(ar.get_element(lista_unidad, z))
            lista_un_dato.append(ar.get_element(lista_fuente, z))

            ar.add_last(lista_final, lista_un_dato)

        if ar.size(lista_final) <= 20:
            ar.add_last(lista_final, [count_total, count_survey, count_census])
            result = lista_final

        else:
            recortada = ar.new_list()
            for i in range(-5,5):
                ar.add_last(recortada, ar.get_element(lista_final, i))
            ar.add_last(recortada, [count_total, count_survey, count_census])
            result = recortada
   

    elif ar.is_empty(lista_datos):
        result = None
    end_time = get_time()
    time = delta_time(start_time, end_time)
    print("\nTiempo " + str(time) + " ms")
    return result

def sort_criteria_3(fecha_1, fecha_2):
    is_sorted = False
    load_time_1 = lp.get(fecha_1, "load_time")
    load_time_2 = lp.get(fecha_2, "load_time")
    if load_time_1 > load_time_2:
        is_sorted = True
    return is_sorted



def req_4(catalog, prod, year_i, year_f):
    start_time = get_time()

    records = ar.new_list()

    index = 0
    count = 0
    count_survey = 0
    count_census = 0

    list_load_time = lp.get(catalog, "load_time")
    list_source = lp.get(catalog, "source")
    list_year = lp.get(catalog, "year_collection")
    list_state = lp.get(catalog, "state_name")
    list_freq = lp.get(catalog, "freq_collection")
    list_unit = lp.get(catalog, "unit_measurement")

    for producto in lp.get(catalog, "commodity")["elements"]:
        producto = producto.replace(" ", "").upper()

        if producto == prod:
            year = int(ar.get_element(list_year, index).replace(" ", ""))
            if year_i <= year <= year_f:
                load_time = dt.strptime(ar.get_element(list_load_time, index), "%Y-%m-%d %H:%M:%S")
                state = ar.get_element(list_state, index).replace(" ", "").upper()
                indice_record = index

                mapa_record = lp.new_map(3, 0.5)
                lp.put(mapa_record, "load_time", load_time)
                lp.put(mapa_record, "state_name", state)
                lp.put(mapa_record, "index", indice_record)

                ar.add_last(records, mapa_record)

                count += 1

                if ar.get_element(list_source, index).replace(" ", "").upper() == "SURVEY":
                    count_survey += 1

                elif ar.get_element(list_source, index).replace(" ", "").upper() == "CENSUS":
                    count_census += 1
        index += 1

    if count == 0:
        return None

    sorted_records = ar.merge_sort(records, sort_criteria_4)
    sorted_indices = ar.new_list()

    for mapa in sorted_records["elements"]:
        ar.add_last(sorted_indices, lp.get(mapa, "index"))

    result = ar.new_list()
    for idx in sorted_indices["elements"]:
        record = []
        record.append(ar.get_element(list_source, idx))
        record.append(ar.get_element(list_year, idx))
        record.append(ar.get_element(list_load_time, idx))
        record.append(ar.get_element(list_freq, idx))
        record.append(ar.get_element(list_state, idx))
        record.append(ar.get_element(list_unit, idx))
        ar.add_last(result, record)

    end_time = get_time()
    delta = str(round(delta_time(start_time, end_time), 2)) + " ms"

    general = ar.new_list()
    ar.add_last(general, delta)
    ar.add_last(general, count)
    ar.add_last(general, count_survey)
    ar.add_last(general, count_census)


    return general, result

def sort_criteria_4(record1, record2):

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

def req_5(catalog, category, year_i, year_f):
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
        anio_actual = int((ar.get_element(list_anios,index)).replace(" ", ""))
        
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
    
    registros_sorteados = ar.merge_sort(registros, sort_criteria_4)
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

def req6(catalog, departamento, año_i, año_f):
    start_time = get_time()
    count_total = 0
    count_survey = 0
    count_census = 0
    lista_años_collección = lp.get(catalog, "year_collection")
    lista_departamento = lp.get(catalog, "state_name")
    lista_fuente = lp.get(catalog, "source")
    lista_frecuencia = lp.get(catalog, "freq_collection")
    lista_producto = lp.get(catalog, "commodity")
    lista_unidad = lp.get(catalog, "unit_measurement")
    lista_carga = lp.get(catalog, "load_time")
    size = ar.size(lista_años_collección)
    lista_datos = ar.new_list()

    for i in range(size):
        if ar.get_element(lista_carga, i) >= str(año_i) and ar.get_element(lista_carga, i) <= str(año_f) and ar.get_element(lista_departamento, i) == departamento:
            load_time = dt.strptime(ar.get_element(lista_carga, i), "%Y-%m-%d %H:%M:%S")
            mapa_aux = lp.new_map(2, 0.5)
            lp.put(mapa_aux, "load_time", load_time)
            lp.put(mapa_aux, "index", i)

            ar.add_last(lista_datos, mapa_aux)

            count_total += 1 
            if ar.get_element(lista_fuente, i) == "SURVEY":
                count_survey += 1
            elif ar.get_element(lista_fuente, i) == "CENSUS":
                count_census += 1

    if not ar.is_empty(lista_datos):
        lista_datos_sorted = ar.merge_sort(lista_datos, sort_criteria_6)
        index_sorted = ar.new_list()
        for j in lista_datos_sorted["elements"]:
            ar.add_last(index_sorted, lp.get(j, "index"))
    
        lista_final = ar.new_list()
        for z in index_sorted["elements"]:
            lista_un_dato = []
            lista_un_dato.append(ar.get_element(lista_fuente, z))
            lista_un_dato.append(ar.get_element(lista_años_collección, z))
            lista_un_dato.append(ar.get_element(lista_carga, z))
            lista_un_dato.append(ar.get_element(lista_frecuencia, z))
            lista_un_dato.append(ar.get_element(lista_departamento, z))
            lista_un_dato.append(ar.get_element(lista_unidad, z))
            lista_un_dato.append(ar.get_element(lista_producto, z))

            ar.add_last(lista_final, lista_un_dato)

    else:
        result = None
    
    if ar.size(lista_final) <= 20:
        ar.add_last(lista_final, [count_total, count_survey, count_census])
        result = lista_final

    else:
        recortada = ar.new_list()
        for i in range(-5,5):
            ar.add_last(recortada, ar.get_element(lista_final, i))
        ar.add_last(recortada, [count_total, count_survey, count_census])
        result = recortada
    end_time = get_time()
    time = delta_time(start_time, end_time)
    print("\nTiempo" + str(time) + "ms")
    return result

def sort_criteria_6(fecha_1, fecha_2):
    is_sorted = False
    load_time_1 = lp.get(fecha_1, "load_time")
    load_time_2 = lp.get(fecha_2, "load_time")
    if load_time_1 > load_time_2:
        is_sorted = True
    return is_sorted


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
    mapa_years = lp.new_map(anio_fin - anio_inicio , 0.5)

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
    delta = str(round(delta_time(start_time, end_time) ,2)) + " ms"

    # Retornar los resultados en orden
    # el return no se como hacerlo con listas tocaria cambiar algunas cosas
    return delta,registros_validos ,registros_invalidos, sorted_years, mayor_ingreso, menor_ingreso, registros_validos, registros_invalidos, total_survey, total_census


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
