import sys
from App import logic as lg
import tabulate as tb   

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

    result = lg.req_1(control, input_year)

    if result is None:
        print(f"\nNo se encontraron registros para el año {input_year}.")
    
    else:
        headers_generales = ["Tiempo de carga", "Registros que cumplieron el filtro"]
        print(tb.tabulate([result[0]["elements"]], headers_generales, tablefmt="pretty"))

        print(f"\nEl último registro recopilado en el año {input_year} es :\n")
        headers = ["Año de recolección", "Fecha de carga", "Fuente", "Frecuencia",  "Estado", "Tipo de producto", "Unidad de medida", "Valor"]
        print(tb.tabulate([result[1]["elements"]], headers, tablefmt="pretty"))

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
    pass


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
    pass


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
