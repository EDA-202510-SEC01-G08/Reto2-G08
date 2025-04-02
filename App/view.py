import sys
from DataStructures.List import array_list as ar
import tabulate as tb
from DataStructures.Map import map_linear_probing as lp
from App import logic as lg
import datetime as dt


default_limit = 1000
sys.setrecursionlimit(default_limit*10)

def new_logic():
    """
        Se crea una instancia del controlador
    """
    #TODO: Llamar la función de la lógica donde se crean las estructuras de datos
    pass

def print_menu():
    print("Bienvenido")
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
    """
    Carga los datos
    """
    #TODO: Realizar la carga de datos
    pass


def print_data(control, id):
    """
        Función que imprime un dato dado su ID
    """
    #TODO: Realizar la función para imprimir un elemento
    pass

def print_req_1(control):
    """
        Función que imprime la solución del Requerimiento 1 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 1
    pass


def print_req_2(control):
    """
        Función que imprime la solución del Requerimiento 2 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 2
    pass


def print_req_3(control):
    """
        Función que imprime la solución del Requerimiento 3 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 3
    pass


def print_req_4(control):
    """
        Función que imprime la solución del Requerimiento 4 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 4
    pass


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
    pass


def print_req_7(control):
    """
        Función que imprime la solución del Requerimiento 7 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 7
    
    departamento = input("\nIngrese el departamento a consultar: ").strip().upper()
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

    if result is None or ar.size(result[1]) == 0:
        print(f"\nNo se encontraron registros para el departamento {departamento} en el rango de años {anio_inicio} - {anio_fin}.")
        return

    headers_generales = ["Tiempo de ejecución", "Registros válidos"]
    print("\nResultados generales:")
    print(tb.tabulate([result[0]["elements"]], headers_generales, tablefmt="pretty"))

    headers = ["Año", "Ingresos", "Registros", "Mayor/Menor", "Registros inválidos", "Survey", "Census"]

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
        inputs = input('Seleccione una opción para continuar\n')
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
