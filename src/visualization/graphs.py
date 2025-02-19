import plotly.express as px
from ../preprocess/dataframe_processor import DataframeProcessor

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

    # Extract hour
    df['hour'] = df['Timestamp (date)'].dt.floor('H')

    # Group by hour and flight status
    traffic_by_hour = df.groupby(['hour', 'Flight status']).size().unstack(fill_value=0)

    # Reset index for plotting
    traffic_by_hour_reset = traffic_by_hour.reset_index()

    # Melt the DataFrame for Plotly
    traffic_melted = traffic_by_hour_reset.melt(id_vars=['hour'], var_name='Flight Status', value_name='Count')

    # Create interactive stacked bar chart using Plotly
    fig = px.bar(
        traffic_melted, 
        y='hour', 
        x='Count', 
        color='Flight Status', 
        title="Hourly Air Traffic (On-Ground vs Airborne)",
        labels={'hour': 'Hour', 'Count': 'Number of Flights'},
        barmode='stack'
    )

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
    fig_hist = px.histogram(df, x="Wait time (s)", nbins=10, title="Wait Time Distribution")
    return fig_hist

def boxplot_wait_times(df):
    fig_box = px.box(df_wait_times, y="Wait time (s)", title="Boxplot de Valores")
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
        labels=dict(x="Hour", y="Date", color="Tiempo de espera (s)"),
        x=heatmap_data.columns,
        y=heatmap_data.index,
        color_continuous_scale="RdYlGn_r",
        title="Mapa de Calor del Tiempo de Espera por Hora y Día del Mes"
    )

    return fig_heatmap
