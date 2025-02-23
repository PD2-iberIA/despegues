<img src="img/logo.jpg" alt="Logo de avión" width="75" style="border-radius: 10px;"/>

# DESPEGUES

<code> GIDIA - Proyecto de datos II </code>

## Índice

1. [Descripción del proyecto](#descripcion)
2. [Instrucciones de instalación](#instalación)
3. [Uso](#uso)
4. [Crédito](#credito)

## 1. Descripción del proyecto
<a name="descripcion"></a>

Este proyecto tiene como objetivo predecir el tiempo que los aviones permanecen en tierra antes de despegar, es decir, cuánto tiempo transcurre entre el punto de espera y el despegue. Para ello se utilizarán datos de tráfico aéreo obtenidos a través de señales **ADS-B** y **Mode S**.

La motivación detrás de este proyecto es mejorar la eficiencia operativa de los aeropuertos, ayudando a predecir y analizar patrones en los tiempos de espera, lo que puede resultar en una mejor planificación de vuelos y reducción de demoras.

Al llevar a cabo este proyecto, aprenderemos a trabajar en equipo, gestionar grandes conjuntos de datos, realizar visualizaciones interactivas y aplicar técnicas de análisis de datos para obtener conclusiones valiosas sobre la eficiencia del tráfico aéreo, además de presentar todo ello públicamente.

### Entrega 1 - Visualización

Para la primera entrega decodificamos y preprocesamos los datos para después construir visualizaciones como histogramas, boxplots y mapas de calor. Buscamos ofrecer un primer acercamiento que permita identificar patrones y tendencias generales en los datos.

## 2. Instrucciones de instalación
<a name="instalacion"></a>

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
<a name="uso"></a>

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

## 4. Crédito
<a name="credito"></a>

Integrantes del equipo *iberIA*:

- Carmen Fernández González
- Javier Martín Fuentes
- Bryan Xavier Quilumba Farinango
- María Romero Huertas
- Carlota Salazar Martín
- Yushan Yang Xu
