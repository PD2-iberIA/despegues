<img src="img/logo.jpg" alt="Logo de avión" width="75" style="border-radius: 10px;"/>

# DESPEGUES

<code> GIDIA - Proyecto de datos II </code>

## Índice

1. [Descripción del proyecto](#1-descripción-del-proyecto)
2. [Instrucciones de instalación](#2-instrucciones-de-instalación)
3. [Uso](#3-instrucciones-de-uso)
4. [Estructura del repositorio](#4-estructura-del-repositorio)
5. [Crédito](#5-crédito)

## 1. Descripción del proyecto

Este proyecto tiene como objetivo predecir el tiempo que los aviones permanecen en tierra antes de despegar, es decir, cuánto tiempo transcurre entre el punto de espera y el despegue. Para ello se utilizarán datos de tráfico aéreo obtenidos a través de señales **ADS-B** y **Mode S**.

Para comprender los datos nos apoyamos en el libro [The 1090 Megahertz Riddle](https://mode-s.org/1090mhz/index.html) que explica de forma detallada cómo funciona la decodificación de este tipo de señales.

La motivación detrás de este proyecto es mejorar la eficiencia operativa de los aeropuertos, ayudando a predecir y analizar patrones en los tiempos de espera, lo que puede resultar en una mejor planificación de vuelos y reducción de demoras.

Al llevar a cabo este proyecto, aprenderemos a trabajar en equipo, gestionar grandes conjuntos de datos, realizar visualizaciones interactivas y aplicar técnicas de análisis de datos para obtener conclusiones valiosas sobre la eficiencia del tráfico aéreo, además de presentar todo ello públicamente.

### Entrega 1 - Visualización

Para la primera entrega decodificamos y preprocesamos los datos para después construir visualizaciones como histogramas, boxplots y mapas de calor. Buscamos ofrecer un primer acercamiento que permita identificar patrones y tendencias generales en los datos.

## 2. Instrucciones de instalación

En primer lugar, clona el repositorio de este proyecto en la carpeta local de tu dispositivo donde desees guardarlo. Utiliza el siguiente comando:

```
git clone <https://github.com/PD2-iberIA/despegues.git>
```

o apóyate en aplicaciones como GitHub Desktop o Git Bash, entre otras.

Para acceder a la carpeta del proyecto, ejecuta la instrucción:

```
cd despegues
```

Por último, instala las dependencias necesarias:

```
pip install -r requirements.txt
```

## 3. Instrucciones de uso

### 3.1. Convenio commits

A la hora de realizar commits, hemos tratado de adoptar el siguiente convenio para facilitar el trabajo en grupo:

- Para mantener la historia del repositorio más legible, si un commit aborda múltiples aspectos o cambios, se deben dividir en commits individuales que traten de separar lo máximo posible estos. Por lo tanto, es preferible realizar commits frecuentes y pequeños en lugar de grandes commits que abarquen múltiples cambios.

- Los commits deben contener código que no rompa la compilación ni la funcionalidad del código anterior.

En general, los commits deben cumplir las siguientes pautas:

- Empezar en imperativo
- No tener punto final
- No superar los 72 caracteres
- No incluir el nombre del fichero que modifican
- Primera palabra capitalizada
- Si es preciso, rellenar la descripción con información adicional
- No poner prefijos del estilo 'ADD, EDIT...'
- Describir el cambio realizado de forma precisa y concisa

## 4. Estructura del repositorio

En la carpeta `src` se encuentra el código principal del proyecto. En su interior se encuentran los siguientes directorios con sus correspondientes módulos:

`preprocess`

En este directorio se encuentran todos los módulos encargados del preprocesamiento de los datos:

- `airport_constants.py`

    Contiene variables constantes con las posiciones de las pistas y el radar.

- `data_processor.py`

    Define la clase *DataProcessor* encargada de preprocesar los datos. Filtra las posiciones de los aviones según si son válidas o no.

- `dataframe_processor.py`

    Define la clase *DataframeProcessor* encargada de realizar operaciones sobre DataFrames de Pandas para después realizar visualizaciones.

- `decoder.py`

    Define la clase *Decoder* encargada de la decodificación de los datos partiendo de los mensajes en base64 enviados por las distintas aeronaves y captados por el radar.

- `parquet_processor.py`

    Define la clase *ParquetProcessor* encargada de procesar los archivos _parquet_.

- `reader.py`

    Incluye funciones para la lectura de los datos. Los datos están almacenados en un archivo _.tar_ y se leen y decodifican por _chunks_, para después ser almacenados como _.parquet_. También incluye una función para aplicar paralelismo y así reducir el tiempo de procesamiento.

- `utilities.py`

    Incluye distintas funciones que nos han sido útiles durante el procesamiento de los datos.

`visualization`

Este directorio contiene los módulos necesarios para la visualización de los datos:

- `dash.py`

    Incluye el código necesario para visualizar las gráficas mediante una aplicación Dash.

- `graphs.py`

    Contiene las funciones necesarias para construir y mostrar las gráficas correspondientes al ejercicio 1 de la entrega.

- `maps.py`

    Define la clase *Maps* que genera disitintos mapas de Folium que pueden ser almacenados como archivos _html_.

## 5. Crédito

Integrantes del equipo *iberIA*:

- Carmen Fernández González
- Javier Martín Fuentes
- Bryan Xavier Quilumba Farinango
- María Romero Huertas
- Carlota Salazar Martín
- Yushan Yang Xu
