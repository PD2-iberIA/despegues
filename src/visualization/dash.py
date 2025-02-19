import dash
from dash import dcc, html
import plotly.express as px
import pandas as pd
import numpy as np


# Crear la aplicación Dash
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Air Traffic Dashboard"),
    dcc.Graph(figure=fig_bar),
    dcc.Graph(figure=fig_hist),
    dcc.Graph(figure=fig_box),
    dcc.Graph(figure=fig_heatmap)
])

if __name__ == '__main__':
    app.run_server(debug=True)