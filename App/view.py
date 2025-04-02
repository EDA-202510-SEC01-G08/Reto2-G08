import sys
from DataStructures.List import array_list as ar
import tabulate as tb
from DataStructures.Map import map_linear_probing as lp
from App import logic as lg
import datetime as dt


default_limit = 1000
sys.setrecursionlimit(default_limit*10)

def new_logic():
    return lg.new_logic()

def print_menu():
    print("\nBienvenido")
    print("1- Cargar información")
    print("2- Ejecutar Requerimiento 1")
    print("3- Ejecutar Requerimiento 2")
    print("4- Ejecutar Requerimiento 3")
    print("5- Ejecutar Requerimiento 4")
    print("6- Ejecutar Requerimiento 5")
    print("7- Ejecutar Requerimiento 6")
    print("8- Ejecutar Requerimiento 7")
    print("9- Ejecutar Requerimiento 8 (Bono)")
    print("0- Salir")

def load_data(control):
    
    file = input("Ingrese el nombre del archivo a cargar: ")
    file_path = f"Data/{file}" 
    
    data = lg.load_data(control, file_path)
    headers_generales = ["Tiempo de carga", "N° de registros", "Año min", "Año max"]
    print(tb.tabulate([data[0]["elements"]], headers_generales, tablefmt="pretty"))    
    print(f"\nLos primeros 5 registros son:\n")
    primeros = data[1]["elements"]
    headers = ["Año de recolección", "Fecha de carga", "Estado", "Fuente", "Unidad de medida", "Valor"]  
    print(tb.tabulate(primeros, headers, tablefmt="pretty"))
    print(f"\nLos últimos 5 registros son:\n")
    primeros = data[2]["elements"]
    headers = ["Año de recolección", "Fecha de carga", "Estado", "Fuente", "Unidad de medida", "Valor"]  
    print(tb.tabulate(primeros, headers, tablefmt="pretty"))

def print_req_1(control):
    
    input_year = input("\nIngrese el año a consultar: ")
    yearss = int(input_year.replace(" ", ""))

    result = lg.req_1(control, yearss)

    if result is None:
        print(f"\nNo se encontraron registros para el año {yearss}.")
    
    else:
        headers_generales = ["Tiempo de carga", "Registros que cumplieron el filtro"]
        print(tb.tabulate([result[0]["elements"]], headers_generales, tablefmt="pretty"))

        print(f"\nEl último registro recopilado en el año {yearss} es :\n")
        headers = ["Año de recolección", "Fecha de carga", "Fuente", "Frecuencia",  "Estado", "Tipo de producto", "Unidad de medida", "Valor"]
        print(tb.tabulate([result[1]["elements"]], headers, tablefmt="pretty"))

def print_req_2(control):
    
    input_state = input("\nIngrese el estado a consultar: ")
    statess = input_state.replace(" ", "")
    statesm = statess.upper()
    input_N = input("\nIngrese el número de registros que quiere listar: ")
    N = int(input_N.replace(" ", ""))
    result = lg.req_2(control, statesm, N)

    if N == 0:
        print("\nEl número de registros no puede ser 0.")

    else:
        result = lg.req_2(control, statesm, N)

        if result is None:
            print(f"\nNo se encontraron registros para el estado {input_state}.")

        else:

            if N > ar.get_element(result[0], 1):
                print(f"\nEl número de registros que se pueden listar es menor a {N}.")
                N = ar.get_element(result[0], 1)
                print(f"\nSe listarán {N} registros.\n")
                
            headers_generales = ["Tiempo de carga", "Registros que cumplieron el filtro"]
            print(tb.tabulate([result[0]["elements"]], headers_generales, tablefmt="pretty"))

            result[1]["elements"] = result[1]["elements"][::-1] #reverso la lista para tener orden 

            print(f"\nLos últimos {input_N} registros cargados del estado {statesm.capitalize()} son:\n")
            headers = ["Año de recolección", "Fecha de carga", "Fuente", "Frecuencia",  "Estado", "Tipo de producto", "Unidad de medida", "Valor"]
            print(tb.tabulate(result[1]["elements"], headers, tablefmt="pretty"))


def print_req_3(control):
    """
        Función que imprime la solución del Requerimiento 3 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 3
    año_i = input("\nIngresa el año inicial del filtro: ")
    año_f = input("\nIngrese el año final del filtro: ")
    estado = input("\nIngrese el estado por el cual desea filtrar: ")
    estado_1 = estado.replace(" ", "")
    estado_2 = estado_1.upper()

    result = lg.req_3(control, año_i, año_f, estado_2)
    if result[1] == None:
        print(f"\nNo se encontraron registos en el estado {estado_2} entre los años {año_i} - {año_f}")
    else:
        headers_general = ["Tiempo (ms)", "Total", "Survey", "Census"]
        print(tb.tabulate([result[0]["elements"]], headers_general, tablefmt="pretty"))
        headers = ["Fuente", "Año de recolección", "Fecha de carga", "Frecuencia de recolección", "Producto", "Unidad de medición"]

        if ar.size(result[1]) <= 20:
            print(f"\nLos registros tomados en el estado {estado_2} dentro del rango de años {año_i} - {año_f} son:\n")
            print(tb.tabulate(result[1]["elements"], headers, tablefmt="pretty"))
        else:
            primeros_5 = ar.sub_list(result[1], ar.size(result[1])-5, 5)
            ultimos_5 = ar.sub_list(result[1], 0, 5)

            print(f"\nLos primeros 5 resgistros tomados en el estado {estado_2} en el rango de años {año_i} - {año_f} son: ")
            print(tb.tabulate(ultimos_5["elements"], headers, tablefmt="pretty"))
            print(f"\nLos últimos 5 resgistros tomados en el estado {estado_2} en el rango de años {año_i} - {año_f} son: ")
            print(tb.tabulate(primeros_5["elements"], headers, tablefmt="pretty"))



def print_req_4(control):
    
    input_product = input("\nIngrese el tipo de producto a consultar: ")
    productss = input_product.replace(" ", "")
    productsm = productss.upper()
    input_year_i = input("\nIngrese el año inicial a consultar: ")
    year_i = int(input_year_i.replace(" ", ""))
    input_year_f = input("\nIngrese el año final a consultar: ")
    year_f = int(input_year_f.replace(" ", ""))

    if year_f > dt.datetime.now().year:
        print("\nNo se puede viajar en el tiempo.")

    elif year_i > year_f:
        print("\nEl año inicial no puede ser mayor al año final.")
    
    else:
        result = lg.req_4(control, productsm, year_i, year_f)

        if result is None:
            print(f"\nNo se encontraron registros para el producto {input_product} en el rango de años {year_i} - {year_f}.")

        else:

            headers_generales = ["Tiempo de carga", "Registros que cumplieron el filtro", "Registros con fuente 'SURVEY' ", "Registros con fuente 'CENSUS' "]
            print(tb.tabulate([result[0]["elements"]], headers_generales, tablefmt="pretty"))

            result[1]["elements"] = result[1]["elements"][::-1] #reverso la lista para tener orden
            headers = ["Fuente", "Año de recolección", "Fecha de carga", "Frecuencia",  "Estado", "Unidad de medida"]

            if ar.size(result[1]) <= 20:
                print(f"\nLos registros que tienen como commodity {productsm} y están en el rango de años {year_i} - {year_f} son:\n")
                print(tb.tabulate(result[1]["elements"], headers, tablefmt="pretty"))
            
            else:
                primeros_5 = ar.sub_list(result[1], 0, 5)
                ultimos_5 = ar.sub_list(result[1], ar.size(result[1])-5, 5)
                print(f"\nLos primeros 5 registros que tienen como commodity {productsm} y están en el rango de años {year_i} - {year_f} son:\n")
                print(tb.tabulate(primeros_5["elements"], headers, tablefmt="pretty"))
                print(f"\nLos últimos 5 registros que tienen como commodity {productsm} y están en el rango de años {year_i} - {year_f} son:\n")
                print(tb.tabulate(ultimos_5["elements"], headers, tablefmt="pretty"))



def print_req_5(control):
    """
        Función que imprime la solución del Requerimiento 5 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 5
    input_cate = input("\nIngrese la categoria a consultar: ")
    categorias = input_cate.replace(" ", "")
    categoriasm = categorias.upper()
    input_year_i = input("\nIngrese el año inicial a consultar: ")
    year_i = int(input_year_i.replace(" ", ""))
    input_year_f = input("\nIngrese el año final a consultar: ")
    year_f = int(input_year_f.replace(" ", ""))

    if year_f > dt.datetime.now().year:
        print("\nNo se puede viajar en el tiempo.")

    elif year_i > year_f:
        print("\nEl año inicial no puede ser mayor al año final.")
    
    else:
        result = lg.req_5(control, categoriasm, year_i, year_f)

        if result is None:
            print(f"\nNo se encontraron registros para la categoria {input_cate} en el rango de años {year_i} - {year_f}.")

        else:

            headers_generales = ["Tiempo de carga", "Registros que cumplieron el filtro", "Registros con fuente 'SURVEY' ", "Registros con fuente 'CENSUS' "]
            print(tb.tabulate([result[0]["elements"]], headers_generales, tablefmt="pretty"))

            result[1]["elements"] = result[1]["elements"][::-1] #reverso la lista para tener orden
            headers = ["Fuente", "Año de recolección", "Fecha de carga", "Frecuencia",  "Estado", "Unidad de medida", "Tipo de dato"]

            if ar.size(result[1]) <= 20:
                print(f"\nLos registros que tienen como statical_category {categoriasm} y están en el rango de años {year_i} - {year_f} son:\n")
                print(tb.tabulate(result[1]["elements"], headers, tablefmt="pretty"))
            
            else:
                primeros_5 = ar.sub_list(result[1], 0, 5)
                ultimos_5 = ar.sub_list(result[1], ar.size(result[1])-5, 5)
                print(f"\nLos primeros 5 registros que tienen como statical_category {categoriasm} y están en el rango de años {year_i} - {year_f} son:\n")
                print(tb.tabulate(primeros_5["elements"], headers, tablefmt="pretty"))
                print(f"\nLos últimos 5 registros que tienen como statical_category {categoriasm} y están en el rango de años {year_i} - {year_f} son:\n")
                print(tb.tabulate(ultimos_5["elements"], headers, tablefmt="pretty"))



def print_req_6(control):
    """
        Función que imprime la solución del Requerimiento 6 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 6
    fecha_i = input("\nIngresa la fecha inicial del filtro en el fromato YYYY-MM-DD: ")
    fecha_f = input("\nIngrese la fecha final del filtro en el fromato YYYY-MM-DD: ")
    estado = input("\nIngrese el estado por el cual desea filtrar: ")
    estado_1 = estado.replace(" ", "")
    estado_2 = estado_1.upper()

    result = lg.req6(control, estado_2, fecha_i, fecha_f)
    if result == None:
        print(f"\nNo se encontraron registos en el estado {estado_2} entre los años {fecha_i} - {fecha_f}")
    else:
        headers = ["Fuente", "Año de recolección", "Fecha de carga", "Frecuencia de recolección", "Estado", "Unidad de medición", "Producto"]

        if ar.size(result) <= 20:
            print(f"\nLos registros tomados en el estado {estado_2} dentro del rango de años {fecha_i} - {fecha_f} son:\n")
            print(tb.tabulate(result["elements"][:-1], headers, tablefmt="pretty"))
            print("\nEl número de registros que cumplen el filtro y tienen como fuente 'survey' son: " + str(result["elements"][-1][1]))
            print("El número de registros que cumplen el filtro y tienen como fuente 'census' son: " + str(result["elements"][-1][2]))
            print("El total de registros que cumplen el filtro son: " + str(result["elements"][-1][0]))

        else:
            primeros_5 = ar.sub_list(result, ar.size(result)-6, 5)
            ultimos_5 = ar.sub_list(result, 0, 5)
            print(f"\nLos primeros 5 registros que tienen como statical_category {estado_2} y están en el rango de años {fecha_i} - {fecha_f} son:\n")
            print(tb.tabulate(ultimos_5["elements"], headers, tablefmt="pretty"))
            print(f"\nLos últimos 5 registros que tienen como statical_category {estado_2} y están en el rango de años {fecha_i} - {fecha_f} son:\n")
            print(tb.tabulate(primeros_5["elements"], headers, tablefmt="pretty"))
            print("\nEl número de registros que cumplen el filtro y tienen como fuente 'survey' son: " + str(result["elements"][-1][1]))
            print("El número de registros que cumplen el filtro y tienen como fuente 'census' son: " + str(result["elements"][-1][2]))
            print("El total de registros que cumplen el filtro son: " + str(result["elements"][-1][0]))




def print_req_7(control):
    """
        Función que imprime la solución del Requerimiento 7 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 7
    
    departamento = input("\nIngrese el estado a consultar: ").strip().upper()
    anio_inicio = int(input("\nIngrese el año inicial a consultar: ").strip())
    anio_fin = int(input("\nIngrese el año final a consultar: ").strip())
    orden = input("\nIngrese el orden (ASCENDENTE o DESCENDENTE): ").strip().upper()

    if anio_fin > dt.datetime.now().year:
        print("\nNo se puede viajar en el tiempo.")
        return

    if anio_inicio > anio_fin:
        print("\nEl año inicial no puede ser mayor al año final.")
        return

    result = lg.req_7(control, departamento, anio_inicio, anio_fin, orden)

    if result is None:
        print(f"\nNo se encontraron registros para el departamento {departamento} en el rango de años {anio_inicio} - {anio_fin}.")
        return

    headers_generales = ["Tiempo de ejecución", "Registros válidos", "Registros inválidos", "Registros con fuente 'SURVEY'", "Registros con fuente 'CENSUS'"]
    print(tb.tabulate([result[0]["elements"]], headers_generales, tablefmt="pretty"))

    if orden == "ASCENDENTE":
        extremo_1 = result[2]["elements"]
        headers_extremos = ["Año", "Ingresos", "Registros", "Mayor / Menor"]
        print(f"\nEl registro con el mayor ingreso en el departamento {departamento} en el rango de años {anio_inicio} - {anio_fin} es:\n")
        print(tb.tabulate([extremo_1], headers_extremos, tablefmt="pretty"))

        headers = ["Año", "Ingresos", "Registros"]

        if ar.size(result[1]) > 15:
            primeros_5 = ar.sub_list(result[1], 0, 5)
            ultimos_5 = ar.sub_list(result[1], ar.size(result[1]) - 5, 5)

            print(f"\nLos primeros 5 registros del departamento {departamento} en el rango de años {anio_inicio} - {anio_fin} son:\n")
            print(tb.tabulate(primeros_5["elements"], headers, tablefmt="pretty"))

            print(f"\nLos últimos 5 registros del departamento {departamento} en el rango de años {anio_inicio} - {anio_fin} son:\n")
            print(tb.tabulate(ultimos_5["elements"], headers, tablefmt="pretty"))
        else:
            print(f"\nLos registros del departamento {departamento} en el rango de años {anio_inicio} - {anio_fin} son:\n")
            print(tb.tabulate(result[1]["elements"], headers, tablefmt="pretty"))

        extremo_2 = result[3]["elements"]
        print(f"\nEl registro con el menor ingreso en el departamento {departamento} en el rango de años {anio_inicio} - {anio_fin} es:\n")
        print(tb.tabulate([extremo_2], headers_extremos, tablefmt="pretty"))
    
    else:
        extremo_1 = result[2]["elements"]
        headers_extremos = ["Año", "Ingresos", "Registros", "Mayor / Menor"]
        print(f"\nEl registro con el menor ingreso en el departamento {departamento} en el rango de años {anio_inicio} - {anio_fin} es:\n")
        print(tb.tabulate([extremo_1], headers_extremos, tablefmt="pretty"))

        headers = ["Año", "Ingresos", "Registros"]

        if ar.size(result[1]) > 15:
            primeros_5 = ar.sub_list(result[1], 0, 5)
            ultimos_5 = ar.sub_list(result[1], ar.size(result[1]) - 5, 5)

            print(f"\nLos primeros 5 registros del departamento {departamento} en el rango de años {anio_inicio} - {anio_fin} son:\n")
            print(tb.tabulate(primeros_5["elements"], headers, tablefmt="pretty"))

            print(f"\nLos últimos 5 registros del departamento {departamento} en el rango de años {anio_inicio} - {anio_fin} son:\n")
            print(tb.tabulate(ultimos_5["elements"], headers, tablefmt="pretty"))
        else:
            print(f"\nLos registros del departamento {departamento} en el rango de años {anio_inicio} - {anio_fin} son:\n")
            print(tb.tabulate(result[1]["elements"], headers, tablefmt="pretty"))

        extremo_2 = result[3]["elements"]
        print(f"\nEl registro con el mayor ingreso en el departamento {departamento} en el rango de años {anio_inicio} - {anio_fin} es:\n")
        print(tb.tabulate([extremo_2], headers_extremos, tablefmt="pretty"))


def print_req_8(control):
    """
        Función que imprime la solución del Requerimiento 8 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 8
    pass


# Se crea la lógica asociado a la vista
control = new_logic()

# main del ejercicio
def main():
    """
    Menu principal
    """
    working = True
    #ciclo del menu
    while working:
        print_menu()
        inputs = input('\nSeleccione una opción para continuar\n')
        if int(inputs) == 1:
            print("Cargando información de los archivos ....\n")
            data = load_data(control)
        elif int(inputs) == 2:
            print_req_1(control)

        elif int(inputs) == 3:
            print_req_2(control)

        elif int(inputs) == 4:
            print_req_3(control)

        elif int(inputs) == 5:
            print_req_4(control)

        elif int(inputs) == 6:
            print_req_5(control)

        elif int(inputs) == 7:
            print_req_6(control)

        elif int(inputs) == 8:
            print_req_7(control)

        elif int(inputs) == 9:
            print_req_8(control)

        elif int(inputs) == 0:
            working = False
            print("\nGracias por utilizar el programa") 
        else:
            print("Opción errónea, vuelva a elegir.\n")
    sys.exit(0)
