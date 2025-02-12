import plotly.express as px

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