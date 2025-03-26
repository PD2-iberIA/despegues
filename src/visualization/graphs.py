import plotly.express as px
from preprocess.dataframe_processor import DataframeProcessor
from preprocess.data_processor import DataProcessor
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, count, unix_timestamp, lag
from pyspark.sql.functions import max as spark_max, min as spark_min, avg, when
from pyspark.sql.window import Window

# Inicializar Spark
spark = SparkSession.builder.appName("FlightVisualization").getOrCreate()

# Gráfica por horas de aviones en tierra y aterrizados
def graph_hourly_flight_status(df_spark):
    """
    Generates an interactive stacked bar chart showing the hourly distribution of flights 
    based on their status (e.g., on-ground or airborne).
    
    Args:
        df_spark (pyspark.sql.DataFrame): Spark DataFrame containing flight data with at least two columns:
            - 'Timestamp (date)': Datetime column representing flight event timestamps.
            - 'Flight status': Categorical column indicating the flight's status.
    
    Returns:
        plotly.graph_objects.Figure: A Plotly figure object displaying the stacked bar chart.
    """
    # Convertir Timestamp a formato datetime
    df_spark = df_spark.withColumn("hour", hour(col("Timestamp (date)")))
    df_spark = df_spark.withColumn("day_of_week", dayofweek(col("Timestamp (date)")))
    
    # Agrupar por hora y estado de vuelo
    df_grouped = df_spark.groupBy("hour", "Flight status").agg(count("Flight status").alias("count"))
    
    # Convertir a pandas para visualización
    df_pandas = df_grouped.toPandas()
    
    # Crear el gráfico
    fig = px.bar(df_pandas, x="count", y="hour", color="Flight status", barmode="stack",
                 labels={"count": "Número de vuelos", "hour": "Hora", "Flight status": "Estado del vuelo"},
                 title="Número de vuelos por hora y estado",
                 color_discrete_sequence=px.colors.qualitative.Plotly,
                 category_orders={"hour": sorted(df_pandas["hour"].unique(), reverse=True)})
    
    # Ajustar diseño
    fig.update_layout(
        xaxis_title="Número de Vuelos",
        yaxis_title="Hora",
        xaxis_tickangle=-45,
        legend_title="Estado del vuelo"
    )
    
    return fig


def df_wait_times(df_spark):
    """
    Genera un DataFrame con los tiempos de espera de vuelos, combinando información 
    de velocidades y estados de vuelo.

    Args:
        df_spark (pyspark.sql.DataFrame): DataFrame con los datos de vuelos, incluyendo información de 
                                          velocidades y estado.

    Returns:
        pyspark.sql.DataFrame: DataFrame procesado con los tiempos de espera.
    """
    df1 = DataframeProcessor.getVelocities(df_spark)
    df2 = DataframeProcessor.getFlights(df_spark)

    df1_s = df1.orderBy(["Timestamp (date)", "ICAO"])
    df2_s = df2.orderBy(["Timestamp (date)", "ICAO"])
    
    t = 600  # 10 minutos en segundos
    df1_s = df1_s.withColumn("timestamp_unix", unix_timestamp(col("Timestamp (date)")))
    df2_s = df2_s.withColumn("timestamp_unix", unix_timestamp(col("Timestamp (date)")))
    
    dff = df1_s.join(df2_s, [df1_s.ICAO == df2_s.ICAO, 
                              abs(df1_s.timestamp_unix - df2_s.timestamp_unix) <= t], 
                     how="inner").drop("timestamp_unix")
    
    return DataProcessor.separate_on_ground_and_airborne(dff)

def histogram_wait_times(df_spark):
    """
    Genera un histograma de la distribución de los tiempos de espera.

    Args:
        df_spark (pyspark.sql.DataFrame): DataFrame con una columna "Wait time (s)".

    Returns:
        plotly.graph_objects.Figure: Figura de Plotly con el histograma generado.
    """
    df_pandas = df_spark.toPandas()
    fig_hist = px.histogram(df_pandas, x="Wait time (s)", nbins=20, title="Wait Time Distribution")
    return fig_hist

def boxplot_wait_times(df_spark):
    """
    Genera un boxplot de los tiempos de espera.

    Args:
        df_spark (pyspark.sql.DataFrame): DataFrame con una columna "Wait time (s)".

    Returns:
        plotly.graph_objects.Figure: Figura de Plotly con el boxplot generado.
    """
    df_pandas = df_spark.toPandas()
    fig_box = px.box(df_pandas, y="Wait time (s)", title="Boxplot de Valores")
    return fig_box

def heatmap_wait_times(df_spark):
    """
    Genera un mapa de calor del tiempo de espera por hora y día del mes.

    Args:
        df_spark (pyspark.sql.DataFrame): DataFrame con las columnas "ts ground" y "Wait time (s)".

    Returns:
        plotly.graph_objects.Figure: Figura de Plotly con el heatmap generado.
    """
    df_spark = df_spark.withColumn("Date", col("ts ground").cast("date"))
    df_spark = df_spark.withColumn("Hour", hour(col("ts ground")))
    
    heatmap_data = df_spark.groupBy("Date", "Hour").avg("Wait time (s)").toPandas()
    
    fig_heatmap = px.imshow(
        heatmap_data.pivot(index="Date", columns="Hour", values="avg(Wait time (s))").values,
        labels=dict(x="Hora", y="Fecha", color="Tiempo de espera (s)"),
        x=heatmap_data["Hour"].unique(),
        y=heatmap_data["Date"].unique(),
        color_continuous_scale="RdYlGn_r",
        title="Mapa de calor del tiempo de espera por hora y día del mes"
    )
    
    return fig_heatmap


def waits_by_categories_and_runways(sdf, umbral):
    """
    Genera visualizaciones de los tiempos de espera en pistas 3 y 4, 
    segmentados por categorías de aeronaves y turbulencia.

    :param sdf (pyspark.sql.DataFrame): DataFrame con información de vuelos.
    :param umbral (int): límite superior de los tiempos de espera.

    :returns None: Genera y muestra gráficos de distribución y comparación de tiempos de espera.
    """

    # Obtener tiempos de espera y categorías de aeronaves
    df_espera = DataframeProcessor.getWaitTimes(sdf)
    df_tipos = DataframeProcessor.getAirplaneCategories(sdf)

    df_aterrizajes = df_espera.filter(col("runway").isin(["3", "4"]))
    df_aterrizajes = df_aterrizajes.join(df_tipos, on="ICAO")

    # Filtrar tiempos de espera menores al umbral
    df_aterrizajes = df_aterrizajes.filter(col("Wait time (s)") < umbral)

    # Ordenar por timestamp de airborne
    df_aterrizajes = df_aterrizajes.orderBy("ts airborne")

    # Obtener categoría de turbulencia del avión anterior en la misma pista
    window_spec = Window.partitionBy("runway").orderBy("ts airborne")
    df_aterrizajes = df_aterrizajes.withColumn("Prev_Turbulence", lag("TurbulenceCategory").over(window_spec))

    # Eliminar valores nulos en Prev_Turbulence
    df_aterrizajes = df_aterrizajes.filter(col("Prev_Turbulence").isNotNull())

    # Convertir a Pandas para visualización
    pdf = df_aterrizajes.toPandas()
    pdf["Prev_Turbulence"] = pdf["Prev_Turbulence"].str.replace(r'\s*\(.*?\)', '', regex=True)
    pdf["TurbulenceCategory"] = pdf["TurbulenceCategory"].str.replace(r'\s*\(.*?\)', '', regex=True)

    # --- Boxplot de tiempo de espera según la turbulencia del avión anterior ---
    plt.figure(figsize=(12, 6))
    sns.boxplot(
        data=pdf,
        y="Prev_Turbulence",
        x="Wait time (s)",
        hue="runway",
        palette="viridis"
    )
    plt.title("Tiempo de espera según la categoría de turbulencia de la aeronave anterior y la pista", fontweight="bold")
    plt.xlabel("Tiempo de espera (s)")
    plt.ylabel("Categoría de turbulencia de la aeronave anterior")
    plt.legend(title="Pista")
    plt.grid(axis="x", linestyle="--", alpha=0.7)
    plt.show()

    # --- Boxplot de tiempo de espera según la turbulencia del propio avión ---
    plt.figure(figsize=(12, 6))
    sns.boxplot(
        data=pdf,
        y="TurbulenceCategory",
        x="Wait time (s)",
        hue="runway",
        palette="viridis"
    )
    plt.title("Tiempo de espera según la categoría de turbulencia de la aeronave y la pista", fontweight="bold")
    plt.xlabel("Tiempo de espera (s)")
    plt.ylabel("Categoría de turbulencia")
    plt.legend(title="Pista")
    plt.grid(axis="x", linestyle="--", alpha=0.7)
    plt.show()

    # --- Distribución de tiempos de espera por pista ---
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 12), sharex=True)

    sns.histplot(
        data=pdf,
        x="Wait time (s)",
        hue="runway",
        kde=True,
        bins=30,
        palette="viridis",
        alpha=0.6,
        stat="density",
        ax=ax1
    )
    ax1.set_title("Tiempo de espera según la pista", fontweight="bold")
    ax1.set_ylabel("Densidad")
    ax1.grid(axis="x", linestyle="--", alpha=0.7)

    sns.boxplot(
        data=pdf,
        x="Wait time (s)",
        y="runway",
        palette="viridis",
        whis=np.inf,
        width=0.4,
        ax=ax2
    )
    ax2.set_xlabel("Tiempo de Espera (s)")
    ax2.set_ylabel("Pista")
    ax2.grid(axis="x", linestyle="--", alpha=0.7)

    plt.show()

def get_flight_stats(sdf, status):
    """ 
    Genera un DataFrame con estadísticas del número máximo, mínimo y medio de vuelos 
    para aviones según su estado.

    :param sdf (pyspark.sql.DataFrame): DataFrame con datos de vuelos, incluyendo columnas 'Flight status', 
                                  'day_of_week', 'count_nonzero' y 'hour'.
    :param status (str): Estado del vuelo ('on-ground' o 'airborne').

    :raises ValueError: Si `status` no es 'on-ground' ni 'airborne'.
    
    :returns pyspark.sql.DataFrame: DataFrame con las estadísticas de vuelos por día de la semana.
    """
    # Verifica que el estado del vuelo sea válido
    if status not in ["on-ground", "airborne"]:
        raise ValueError("El parámetro 'status' debe ser 'on-ground' o 'airborne'.")

    # Filtrar el DataFrame por estado
    sdf_filtered = sdf.filter(col("Flight status") == status)

    # Calcular estadísticas
    sdf_stats = sdf_filtered.groupBy("day_of_week").agg(
        spark_max("count_nonzero").alias("max_count"),
        spark_min("count_nonzero").alias("min_count"),
        avg(when(col("Flight status") == "airborne", col("count_nonzero"))).alias("avg_airborne"),
        avg(when(col("Flight status") == "on-ground", col("count_nonzero"))).alias("avg_on_ground")
    )

    # Agregar la hora correspondiente a los valores máximos y mínimos
    sdf_max_hour = sdf_filtered.groupBy("day_of_week", "hour").agg(spark_max("count_nonzero").alias("max_count"))
    sdf_min_hour = sdf_filtered.groupBy("day_of_week", "hour").agg(spark_min("count_nonzero").alias("min_count"))

    sdf_stats = sdf_stats.join(sdf_max_hour, ["day_of_week"], "left")
    sdf_stats = sdf_stats.withColumnRenamed("hour", "max_hour")
    sdf_stats = sdf_stats.join(sdf_min_hour, ["day_of_week"], "left")
    sdf_stats = sdf_stats.withColumnRenamed("hour", "min_hour")

    # Eliminar la columna del estado contrario
    if status == "airborne":
        sdf_stats = sdf_stats.drop("avg_on_ground")
    else:
        sdf_stats = sdf_stats.drop("avg_airborne")

    return sdf_stats



