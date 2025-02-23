import plotly.express as px
from preprocess.dataframe_processor import DataframeProcessor
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

# Gráfica por horas de aviones en tierra y aterrizados
def graph_hourly_flight_status(df):
    """
    Generates an interactive stacked bar chart showing the hourly distribution of flights 
    based on their status (e.g., on-ground or airborne).

    Args:
        df (pd.DataFrame): A DataFrame containing flight data with at least two columns:
            - 'Timestamp (date)': Datetime column representing flight event timestamps.
            - 'Flight status': Categorical column indicating the flight's status.

    Returns:
        plotly.graph_objects.Figure: A Plotly figure object displaying the stacked bar chart.
    """
    # Ensure timestamp is in datetime format
    df['Timestamp (date)'] = pd.to_datetime(df['Timestamp (date)'])

    df["day_of_week"] = df["hour"].dt.day_name()

    # Extraer solo la hora sin fecha
    df["hour"] = df["hour"].dt.strftime('%H:00')

    # Group by hour and flight status
    #traffic_by_hour = df.groupby(['hour', 'Flight status']).size().unstack(fill_value=0)

    # Reset index for plotting
    #traffic_by_hour_reset = traffic_by_hour.reset_index()

    # Melt the DataFrame for Plotly
    #traffic_melted = traffic_by_hour_reset.melt(id_vars=['hour'], var_name='Flight Status', value_name='Count')

    
    # Agrupar por hora y estado de vuelo
    df_grouped = df.groupby(["hour", "Flight status"], as_index=False).sum()

    # Crear el gráfico de barras apiladas horizontal con eje y invertido
    fig = px.bar(df_grouped, x="count_nonzero", y="hour", color="Flight status", barmode="stack",
                labels={"count_nonzero": "Número de vuelos", "hour": "Hora", "Flight status": "Estado del vuelo"},
                title="Número de vuelos por hora y estado",
                color_discrete_sequence=px.colors.qualitative.Plotly,
                category_orders={"hour": df_grouped["hour"].sort_values(ascending=False)})

    # Update layout for better visualization
    fig.update_layout(
        xaxis_title="Hour",
        yaxis_title="Number of Flights",
        xaxis_tickangle=-45,
        legend_title="Flight Status"
    )

    return fig

# Sacar df de los tiempos de espera
def df_wait_times(df):
    # Extraemos las columnas necesarias y creamos un df nuevo
    df1 = DataframeProcessor.getVelocities(df)
    df2 = DataframeProcessor.getFlights(df)

    df1_s = df1.sort_values(["Timestamp (date)", "ICAO"])
    df2_s = df2.sort_values(["Timestamp (date)", "ICAO"])

    t = pd.Timedelta('10 minute')
    dff = pd.merge_asof(df1_s, df2_s, on="Timestamp (date)", by="ICAO", direction="nearest", tolerance=t)

    # Ensure data is sorted by Flight ID and timestamp
    dff = dff.sort_values(by=["Callsign", "Timestamp (date)"])

    # Separate on-ground and airborne events
    on_ground = dff[(dff["Flight status"] == "on-ground") & (dff["Speed"]==0)].groupby("Callsign")["Timestamp (date)"].min()
    airborne = dff[dff["Flight status"] == "airborne"].groupby("Callsign")["Timestamp (date)"].min()

    # Create new dataframes and rename timestamp columns
    on_ground = pd.DataFrame(on_ground)
    on_ground.columns = ["ts ground"]

    airborne = pd.DataFrame(airborne)
    airborne.columns = ["ts airborne"]

    # Merge them into a new dataframe and extract the waiting seconds
    df_wait_times = on_ground.merge(airborne, how="inner", on="Callsign")
    df_wait_times = df_wait_times[df_wait_times["ts airborne"] > df_wait_times["ts ground"]]
    df_wait_times["Wait time"] = df_wait_times["ts airborne"] - df_wait_times["ts ground"]
    df_wait_times["Wait time (s)"] = df_wait_times["Wait time"].dt.total_seconds()

    return df_wait_times

def histogram_wait_times(df):
    fig_hist = px.histogram(df, x="Wait time (s)", nbins=20, title="Wait Time Distribution")
    return fig_hist

def boxplot_wait_times(df):
    fig_box = px.box(df, y="Wait time (s)", title="Boxplot de Valores")
    return fig_box

def heatmap_wait_times(df):
    # Extraer fecha y hora
    df["Date"] = df["ts ground"].dt.date
    df["Hour"] = df["ts ground"].dt.hour

    # Crear tabla pivote para el heatmap
    heatmap_data = df.pivot_table(index="Date", columns="Hour", values="Wait time (s)", aggfunc="mean")

    # Crear el mapa de calor con Plotly
    fig_heatmap = px.imshow(
        heatmap_data.values,
        labels=dict(x="Hora", y="Fecha", color="Tiempo de espera (s)"),
        x=heatmap_data.columns,
        y=heatmap_data.index,
        color_continuous_scale="RdYlGn_r",
        title="Mapa de calor del tiempo de espera por hora y día del mes"
    )

    return fig_heatmap


def waits_by_categories_and_runways(df):

    df_espera = DataframeProcessor.getWaitTimes(df)
    df_aterrizajes = df_espera[df_espera["runway"].isin(["3","4"])]
    df_tipos = DataframeProcessor.getAirplaneCategories(df)
    df_aterrizajes = df_aterrizajes.merge(df_tipos, on="ICAO")

    df_aterrizajes = df_aterrizajes.sort_values(by="ts airborne")

    # - Boxplot de tiempo de espera según la categoría del avión anterior y la pista -

    df_aterrizajes2 = df_aterrizajes.sort_values(by="ts airborne")

    df_aterrizajes2["Prev_Turbulence"] = df_aterrizajes2.groupby("runway")["TurbulenceCategory"].shift(1)
    df_aterrizajes2 = df_aterrizajes2.dropna(subset=["Prev_Turbulence"])

    df_aterrizajes2["Prev_Turbulence"] = df_aterrizajes2["Prev_Turbulence"].str.replace(r'\s*\(.*?\)', '', regex=True)

    df_aterrizajes2["Prev_Turbulence"] = pd.Categorical(
        df_aterrizajes2["Prev_Turbulence"], 
        categories=sorted(df_aterrizajes2["Prev_Turbulence"].unique()), 
        ordered=True
    )

    plt.figure(figsize=(12, 6))
    sns.boxplot(
        data=df_aterrizajes2,
        y="Prev_Turbulence",
        x="Wait time (s)",
        hue="runway",
        palette=sns.color_palette("viridis", n_colors=len(df_aterrizajes["runway"].unique()))
    )

    plt.title("Tiempo de espera según la categoría de turbulencia de la aeronave anterior y la pista", fontweight="bold")
    plt.suptitle("2024-12-07", fontsize=12, color="gray", y=0.95)
    plt.xlabel("Tiempo de espera (s)", fontsize=12, color="gray")
    plt.ylabel("Categoría de turbulencia de la aeronave anterior", fontsize=12, color="gray")
    plt.legend(title="Pista")
    plt.grid(axis="x", linestyle="--", alpha=0.7)

    plt.show()

    # - Boxplot de tiempo de espera según la categoría del avión anterior y la pista -

    df_aterrizajes3 = df_aterrizajes.copy()

    df_aterrizajes3["TurbulenceCategory"] = df_aterrizajes3["TurbulenceCategory"].str.replace(r'\s*\(.*?\)', '', regex=True)

    df_aterrizajes3["TurbulenceCategory"] = pd.Categorical(
        df_aterrizajes3["TurbulenceCategory"], 
        categories=sorted(df_aterrizajes3["TurbulenceCategory"].unique()), 
        ordered=True
    )

    plt.figure(figsize=(12, 6))
    sns.boxplot(
        data=df_aterrizajes3,
        y="TurbulenceCategory",
        x="Wait time (s)",
        hue="runway",
        palette=sns.color_palette("viridis", n_colors=len(df_aterrizajes["runway"].unique()))
    )

    plt.title("Tiempo de espera según la categoría de turbulencia de la aeronave y la pista", fontweight="bold")
    plt.suptitle("2024-12-07", fontsize=12, color="gray", y=0.95)
    plt.xlabel("Tiempo de espera (s)", fontsize=12, color="gray")
    plt.ylabel("Categoría de turbulencia", fontsize=12, color="gray")
    plt.legend(title="Pista")
    plt.grid(axis="x", linestyle="--", alpha=0.7)

    plt.show()


    # - Distribución de tiempos de espera por pista -
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 12), sharex=True)

    # Crear el histograma por pista
    sns.histplot(
        data=df_aterrizajes,
        x="Wait time (s)",
        hue="runway",
        kde=True,  # Añadir la estimación de densidad
        bins=30,  # Número de bins
        palette=sns.color_palette("viridis", n_colors=len(df_aterrizajes["runway"].unique())),
        alpha=0.6,
        stat="density",  # Usar densidad para normalizar el histograma
        ax=ax1  # Especificar el eje
    )

    ax1.set_title("Tiempo de espera según la pista", fontweight="bold")
    ax1.set_ylabel("Densidad", fontsize=12, color="gray")
    ax1.grid(axis="x", linestyle="--", alpha=0.7)


    # Crear el boxplot
    sns.boxplot(
        data=df_aterrizajes,
        x="Wait time (s)",
        y="runway",
        palette=sns.color_palette("viridis", n_colors=len(df_aterrizajes["runway"].unique())),
        whis=np.inf,  # Para mostrar todos los puntos
        width=0.4,  # Ancho del boxplot
        ax=ax2  # Especificar el eje
    )

    # Añadir títulos y etiquetas
    ax2.set_xlabel("Tiempo de Espera (s)", fontsize=12, color="gray")
    ax2.set_ylabel("Pista", fontsize=12, color="gray")
    ax2.grid(axis="x", linestyle="--", alpha=0.7)

    fig.suptitle("2024-12-07", fontsize=12, color="gray", y=0.92)

    plt.show()
