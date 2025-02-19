import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import pandas as pd
import numpy as np
import graphs

# aqui habría q llenar el código para procesar los df de fm adecuada con las funciones correspondientes
# --------------------

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
    dcc.Graph(id='heatmap-graph', figure=heatmap_wait_times(df_wait_times))
])

# Callback para actualizar gráficos basados en el día seleccionado
@app.callback(
    [Output('bar-graph', 'figure'),
     Output('hist-graph', 'figure'),
     Output('box-graph', 'figure')],
     
    [Input('day-dropdown', 'value')]
)
def update_graphs(selected_day):
    df1 = df[df['day_of_week'] == selected_day]
    df2 = df_wait_times[df_wait_times['day_of_week'] == selected_day]
    
    fig_bar = graph_hourly_flight_status(df1)
    fig_hist = histogram_wait_times(df2)
    fig_box = boxplot_wait_times(df2)

    return fig_bar, fig_hist, fig_box

if __name__ == '__main__':
    app.run_server(debug=True)