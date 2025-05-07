<img src="img/logo.jpg" alt="Logo de avión" width="75" style="border-radius: 10px;"/>

# DESPEGUES

<code> GIDIA - Proyecto de datos II </code>

## Índice

1. [Descripción del proyecto](#1-descripción-del-proyecto)

   1.1 [Documentación](#11-documentación)
3. [Instrucciones de instalación](#2-instrucciones-de-instalación)
4. [Estructura del código](#3-estructura-del-código)
5. [Crédito](#4-crédito)

## 1. Descripción del proyecto

Este proyecto tiene como objetivo predecir el tiempo que los aviones permanecen en tierra antes de despegar, es decir, cuánto tiempo transcurre entre el punto de espera y el despegue. Para ello se utilizarán datos de tráfico aéreo obtenidos a través de señales **ADS-B** y **Mode S**.

Para comprender los datos nos apoyamos en el libro [The 1090 Megahertz Riddle](https://mode-s.org/1090mhz/index.html) que explica de forma detallada cómo funciona la decodificación de este tipo de señales.

La motivación detrás de este proyecto es mejorar la eficiencia operativa de los aeropuertos, ayudando a predecir y analizar patrones en los tiempos de espera, lo que puede resultar en una mejor planificación de vuelos y reducción de demoras.

Al llevar a cabo este proyecto, aprenderemos a trabajar en equipo, gestionar grandes conjuntos de datos, realizar visualizaciones interactivas y aplicar técnicas de análisis de datos para obtener conclusiones valiosas sobre la eficiencia del tráfico aéreo, además de presentar todo ello públicamente.

#### 1.1 Documentación
Para más detalles, puedes consultar toda la documentación en la [Wiki](https://github.com/PD2-iberIA/despegues/wiki) de nuestro repositorio.

### Entrega 1 - Visualización

Para la primera entrega decodificamos y preprocesamos los datos para después construir visualizaciones como histogramas, boxplots y mapas de calor. Buscamos ofrecer un primer acercamiento que permita identificar patrones y tendencias generales en los datos.

### Entrega 2 - Entrenamiento

En la segunda entrega, partiendo del conjunto de datos completo (comprende unos 3 meses), tratamos de entrenar modelos para, dado un avión parado en un punto de espera del aeropuerto, predecir en cuántos segundos despegará. También construímos modelos probabilistas para clasificar los aviones según su categoría de turbulencia, calcular la probabilidad de que un avión haya despegado en el siguiente estado...


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

## 3. Estructura del código

```
📦 despegues
├─ .gitignore
├─ README.md
├─ data
├─ img
├─ requirements.txt
└─ src
   ├─ exploratory_analysis
   │  ├─ datosICAO.ipynb
   │  ├─ demo_decoder.ipynb
   │  ├─ variable_exploration.ipynb
   │  └─ visualization
   │     ├─ custom_icons
   │     │  ├─ radar_icon.png
   │     │  └─ runway_icon.png
   │     ├─ dash.py
   │     ├─ graphs.py
   │     ├─ maps.py
   │     └─ visualization_e1.ipynb
   ├─ models
   │  ├─ probabilistic_models
   │  │  ├─ classifications
   │  │  │  ├─ model_categoria_turb.ipynb
   │  │  │  └─ model_dia_semana.ipynb
   │  │  └─ takeoff_prediction
   │  │     ├─ model_HMM.ipynb
   │  │     └─ model_Naive_Bayes.ipynb
   │  └─ takeoff_time_prediction
   │     ├─ model_transformer.ipynb
   │     ├─ model_dual_transformer.ipynb
   │     ├─ evaluation
   │     │  └─ evaluator.py
   │     ├─ mlflow_experiments
   │     ├─ model_LSTM.ipynb
   │     ├─ model_TEMPLATE.ipynb
   │     ├─ model_XG_boost.ipynb
   │     ├─ model_ada_boost.ipynb
   │     ├─ model_dense_nw.ipynb
   │     ├─ model_dense_nw_by_runway.ipynb
   │     ├─ model_heuristic_1.ipynb
   │     ├─ model_heuristic_2.ipynb
   │     ├─ model_linear_regression.ipynb
   │     └─ model_random_forest.ipynb
   ├─ preprocess
   │  ├─ cleaning
   │  │  ├─ cleaner.py
   │  │  └─ cleaning.ipynb
   │  ├─ cluster
   │  │  ├─ convert.py
   │  │  ├─ pipeline_decoder.json
   │  │  └─ pipeline_preprocess.json
   │  ├─ decoding
   │  │  ├─ data_reader.py
   │  │  ├─ decoder.py
   │  │  ├─ parquet_processor.py
   │  │  └─ reader.py
   │  ├─ enrichment
   │  │  ├─ airport_constants.py
   │  │  ├─ data_processor.py
   │  │  ├─ dataframe_processor.py
   │  │  ├─ geometry_processor.ipynb
   │  │  ├─ holidays_to_json.py
   │  │  ├─ meteo.py
   │  │  ├─ pipeline.ipynb
   │  │  ├─ pipeline.py
   │  │  └─ utilities.py
   │  └─ train_test_split.ipynb
   └─ test
      ├─ predictions.ipynb
      └─ preprocess_scenarios.ipynb
```

## `src`

La carpeta `src` contiene el **código fuente principal del proyecto**, organizado en subdirectorios que reflejan distintas fases del flujo de trabajo: preprocesamiento, modelado, análisis exploratorio, visualización y pruebas. A continuación, se describe la función de cada subdirectorio:



---

### `preprocess/`

Contiene todos los **scripts y notebooks dedicados al preprocesamiento** de datos brutos, enriquecimiento, y transformación.

- **`cluster/`**
  - Pipelines empleados para decodificar y preprocesar los datos en el clúster Cloudera.

- **`decoding/`**
  - `decoder.py`: Clase *Decoder* para transformar mensajes de radar en datos estructurados.
  - `reader.py`, `parquet_processor.py`, `data_reader.py`: Funciones para leer, procesar y guardar datos en formato `.parquet`.

- **`enrichment/`**
  - Scripts para agregar contexto adicional a los datos: meteorología, feriados, posiciones geográficas.
  - `airport_constants.py`: Posiciones fijas del radar y pistas.
  - `pipeline.py`: Script principal que integra todo el proceso de enriquecimiento.

- **`cleaning/`**
  - Scripts y notebooks para limpiar y validar los datos.

- **`train_test_split.ipynb`**  
  Notebook para dividir los datos en conjuntos de entrenamiento y prueba.

---

### `exploratory_analysis/`

Contiene notebooks usados para explorar los datos en las etapas iniciales del proyecto. Incluye análisis preliminares, pruebas de decodificación, y visualización básica.

- **`datosICAO.ipynb` / `demo_decoder.ipynb` /**  
  Notebooks con exploraciones específicas sobre variables, formatos de datos, y decodificación.

- **`visualization/`**  
  Subcarpeta con los módulos de visualización usados para la Entrega I.
  - `dash.py`, `graphs.py`, `maps.py`: Scripts para generar dashboards interactivos y mapas.

- **`variable_exploration.ipynb`**  
    Exploración de variables del conjunto de datos final.

---

### `models/`

Contiene todos los notebooks y scripts relacionados con la **creación, entrenamiento y evaluación de modelos**.

- **`probabilistic_models/`**
  - `classifications/`: Modelos probabilísticos simples como Naive Bayes para clasificar categorías de despegue o días de semana.
  - `takeoff_prediction/`: Modelos secuenciales como HMM para predecir el momento del despegue.

- **`takeoff_time_prediction/`**
  - Incluye modelos de regresión (lineal, XGBoost, Random Forest, redes neuronales, heurísticos).
  - `evaluation/evaluator.py`: Contiene el código necesario para visualizar un dashboard de Plotly con distintas gráficas. Este dashboard puede ser ejecutado con cualquiera de los modelos entrenados.
  - `mlflow_experiments/`: Directorio de experimentación con MLflow para seguimiento de ejecuciones.
  - `model_*.ipynb`: Cada uno de estos notebooks corresponde a un modelo diferente. Todos los notebooks siguen una plantilla por lo que son similares. Se diferencian principalmente en el preprocesamiento de los datos y el entrenamiento y evaluación del modelo, que son aspectos diferenciados según el modelo.

---

### `test/`

Notebooks para **generar las predicciones de los escenarios**:

- `predictions.ipynb`: Verifica las predicciones generadas por modelos.
- `preprocess_scenarios.ipynb`: Evalúa escenarios específicos del flujo.

## 4. Crédito

Integrantes del equipo *iberIA*:

- Carmen Fernández González
- Javier Martín Fuentes
- Bryan Xavier Quilumba Farinango
- María Romero Huertas
- Carlota Salazar Martín
- Yushan Yang Xu
