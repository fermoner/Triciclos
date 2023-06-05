from pyspark import SparkContext
import sys

def interpretar_arista(linea):
    n1, n2 = linea.strip().strip('"').split('","')
    if n1 < n2:
        return (n1, n2)
    elif n1 > n2:
        return (n2, n1)
    else:
        return None

def mapear_aristas_a_lista_adyacencia(arista):
    if arista is not None:
        n1, n2 = arista
        return [(n1, [n2]), (n2, [n1])]
    else:
        return []

def reducir_lista_adyacencia(lista1, lista2):
    return lista1 + lista2

def encontrar_ciclos_de_3(lista_adyacencia, diccionario_adyacencia):
    nodo, vecinos = lista_adyacencia
    vecinos.sort()
    ciclos = []
    for i in range(len(vecinos)):
        for j in range(i + 1, len(vecinos)):
            n1, n2 = vecinos[i], vecinos[j]
            if n2 in diccionario_adyacencia.get(n1, []):
                ciclos.append(tuple(sorted([nodo, n1, n2])))
    return ciclos

def main(sc, nombre_archivo):
    aristas = sc.textFile(nombre_archivo).map(interpretar_arista)
    listas_adyacencia = aristas.flatMap(mapear_aristas_a_lista_adyacencia).reduceByKey(reducir_lista_adyacencia)

    diccionario_adyacencia = listas_adyacencia.collectAsMap()

    ciclos_de_3 = listas_adyacencia.flatMap(lambda lista_adyacencia: encontrar_ciclos_de_3(lista_adyacencia, diccionario_adyacencia)).distinct().collect()

    print('Ciclos de 3:', ciclos_de_3)

if __name__ == "__main__":
    with SparkContext() as sc:
        main(sc, sys.argv[1])
