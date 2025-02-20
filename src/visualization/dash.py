import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import pandas as pd
import numpy as np
import graphs as g
import preprocess.utilities as ut
from preprocess.dataframe_processor import DataframeProcessor

DATA_PATH = ""
df = pd.read_parquet(DATA_PATH)

# Procesamos los datos de la forma adecuada
df = ut.stringToNan(df)
df = ut.extractHour(df)
df = ut.extractDaysOfTheWeek(df)

# Creamos el df para el diagrama de barras de aviones aterrizados vs en vuelo
df_status = DataframeProcessor.getFlightStatus(df)

# Creamos el df para las gráficas de tiempo de espera
df_wait_times = DataframeProcessor.getWaitTimes(df)

# Crear la aplicación Dash
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Air Traffic Dashboard"),
    
    dcc.Dropdown(
        id='day-dropdown',
        options=[{'label': day, 'value': day} for day in df['day_of_week'].unique()],
        value=df['day_of_week'].unique()[0],  # Valor por defecto
        clearable=False
    ),
    
    dcc.Graph(id='bar-graph'),
    dcc.Graph(id='hist-graph'),
    dcc.Graph(id='box-graph'),
    dcc.Graph(id='heatmap-graph', figure=g.heatmap_wait_times(df_wait_times))
])

# Callback para actualizar gráficos basados en el día seleccionado
@app.callback(
    [Output('bar-graph', 'figure'),
     Output('hist-graph', 'figure'),
     Output('box-graph', 'figure')],
     
    [Input('day-dropdown', 'value')]
)
def update_graphs(selected_day):
    df1 = df_status[df_status['day_of_week'] == selected_day]
    df2 = df_wait_times[df_wait_times['day_of_week'] == selected_day]
    
    fig_bar = g.graph_hourly_flight_status(df1)
    fig_hist = g.histogram_wait_times(df2)
    fig_box = g.boxplot_wait_times(df2)

    return fig_bar, fig_hist, fig_box

if __name__ == '__main__':
    app.run_server(debug=True)