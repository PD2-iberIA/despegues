import dash
import dash_core_components as dcc
from dash import html
from dash.dependencies import Output, Input
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
from plotly.subplots import make_subplots
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import numpy as np

class Evaluator:
    def __init__(self, df, model_name="X", mae_val=None, rmse_val=None):
        self.df = df.copy()
        self.model_name = model_name
        self.mae_val = mae_val
        self.rmse_val = rmse_val
        if 'timestamp' in self.df.columns:
            self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
            self.df['date'] = self.df['timestamp'].dt.date

        self.df['error'] = self.df['prediction'] - self.df['takeoff_time']
        self.df['abs_error'] = self.df['error'].abs()
        self.df['squared_error'] = self.df['error'] ** 2

        # Calcular MAPE
        self.df['mape'] = 100 * self.df['abs_error'] / self.df['takeoff_time'].replace(0, 1)
        self.mape = np.mean(self.df['mape'])  # Promedio de MAPE

        numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    
        # Actualizar las opciones del dropdown
        self.error_variable_options = [{'label': col, 'value': col} for col in numeric_columns]

        # Holding points
        self.holding_points = [
            ("Z1", "36L/18R", -3.573093114831562, 40.490653160130186),
            ("KA6", "32R/14L", -3.537524367869231, 40.472076292010001),
            ("KA8", "32R/14L", -3.536653485337274, 40.466622566754253),
            ("K3", "32R/14L", -3.558959606954449, 40.494122669084419),
            ("K2", "32R/14L", -3.559326044131887, 40.4945961819448),
            ("K1", "32R/14L", -3.560411408421098, 40.495554592925956),
            ("Y1", "36R/18L", -3.560656492186808, 40.499431092287409),
            ("Y2", "36R/18L", -3.560645785166937, 40.500298406173002),
            ("Y3", "36R/18L", -3.560660061193443, 40.501183565039504),
            ("Y7", "36R/18L", -3.560800449906033, 40.533391949745102),
            ("Z6", "36L/18R", -3.576129307304151, 40.495184843931881),
            ("Z4", "36L/18R", -3.576034129003182, 40.492555539298088),
            ("Z2", "36L/18R", -3.575903257941006, 40.491865496230126),
            ("Z3", "36L/18R", -3.57319305240692, 40.491819096186241),
            ("LF", "32L/14R", -3.572566658955927, 40.479721203031424),
            ("L1", "32L/14R", -3.57652786733783, 40.483565816902733),
            ("LA", "32L/14R", -3.577181028787666, 40.484251101106899),
            ("LB", "32L/14R", -3.577553413710587, 40.484873329796898),
            ("LC", "32L/14R", -3.575750378154376, 40.486690643924192),
            ("LD", "32L/14R", -3.575150753600524, 40.486522892072891),
            ("LE", "32L/14R", -3.574915186586964, 40.485580625293494)
        ]
        self.holding_points_df = pd.DataFrame(self.holding_points, columns=["holding_point", "runway", "lon", "lat"])

        # Coordenadas de las pistas
        self.runways = [
            ("32L/14R", -3.575695, 40.484656),
            ("32R/14L", -3.558139, 40.495018),
            ("36L/18R", -3.574648, 40.501333),
            ("36R/18L", -3.559239,  40.504447)
        ]
        self.runways_df = pd.DataFrame(self.runways, columns=["runway", "lon", "lat"])


    def getReport(self):
        report = {}

        # Global metrics
        mae = mean_absolute_error(self.df['takeoff_time'], self.df['prediction'])
        mse = mean_squared_error(self.df['takeoff_time'], self.df['prediction'])
        rmse = np.sqrt(mse)
        r2 = r2_score(self.df['takeoff_time'], self.df['prediction'])
        mape = self.mape

        report['global'] = {
            'mae': mae,
            'rmse': rmse,
            'mse': mse,
            'r2': r2,
            'mape': mape
        }

        # Metrics by runway
        report['by_runway'] = {}
        for runway, group in self.df.groupby('runway'):
            group_mae = mean_absolute_error(group['takeoff_time'], group['prediction'])
            group_rmse = np.sqrt(mean_squared_error(group['takeoff_time'], group['prediction']))
            report['by_runway'][runway] = {
                'mae': group_mae,
                'rmse': group_rmse
            }

        # Metrics by holding_point
        report['by_holding_point'] = {}
        for hp, group in self.df.groupby('holding_point'):
            group_mae = mean_absolute_error(group['takeoff_time'], group['prediction'])
            group_rmse = np.sqrt(mean_squared_error(group['takeoff_time'], group['prediction']))
            report['by_holding_point'][hp] = {
                'mae': group_mae,
                'rmse': group_rmse
            }

        return report

    def visualEvaluation(self):
        app = dash.Dash(__name__)

        app.layout = html.Div([
            
            html.Div([
                html.H1("Evaluación"),
            ], className="title-container"),
            html.Div(f"{self.model_name}", className="model-name"),
                    
            html.Div(
                    [
                        dcc.RadioItems(
                            id='mode-selector',
                            options=[
                                {'label': 'Por pista', 'value': 'runway'},
                                {'label': 'Por punto de espera', 'value': 'holding_point'}
                            ],
                            value='runway'
                        ),
                        dcc.RadioItems(
                            id='error-metric-selector',
                            options=[
                                {'label': 'MAE', 'value': 'MAE'},
                                {'label': 'RMSE', 'value': 'RMSE'}
                            ],
                            value='MAE'
                        )
                    ], className='sticky-radio-items-container'
            ),

            html.H2("Métricas globales"),
            dcc.Graph(id='global-error'),

            html.Div([
                html.Div([
                    html.H2("MAE y RMSE por zonas"),
                    dcc.Graph(id='error-by-mode', style={'flex': 1})
                ], style={'flex': 1, 'padding-right': '20px'}),  # Ajusta el espacio entre los gráficos si es necesario
                
                html.Div([
                    html.H2("Mapa de calor por día y hora"),
                    dcc.Graph(id='heatmap-error', style={'flex': 1})
                ], style={'flex': 1})
            ], style={'display': 'flex', 'justify-content': 'space-between'}),


            html.H2("Distribución del error global"),
            dcc.Graph(id='histogram-error'),

            html.H2("Error en función de una variable"),
            dcc.Dropdown(
                id='dropdown-error-time',
                options=self.error_variable_options,
                value='time_at_holding_point',
                style={'width': '50%'}
            ),
    
            html.Div([
                dcc.Graph(id='error-vs-variable', style={'flex': 1}),
                dcc.Graph(id='scatter-plot', style={'flex': 1})
            ], style={'display': 'flex', 'justify-content': 'space-between'}),


            html.Div([
                html.Div([
                    html.H2("Distribución del error por zonas"),
                    dcc.Graph(id='boxplot-error', style={'flex': 1})
                ], style={'flex': 1, 'padding-right': '20px'}),  # Espacio entre los gráficos
                
                html.Div([
                    html.H2("Métrica por zonas"),
                    dcc.Graph(id='geo-error', style={'flex': 1})
                ], style={'flex': 1})
            ], style={'display': 'flex', 'justify-content': 'space-between'}),


            html.H2("Error por categorías de turbulencia"),
            dcc.Graph(id='boxplot-turbulence'),

            html.H2("Evolución de las métricas diarias"),
            dcc.Graph(id='temporal-error'),
        ])

        @app.callback(
            Output('global-error', 'figure'),
            Output('histogram-error', 'figure'),
            Output('heatmap-error', 'figure'),
            Output('error-vs-variable', 'figure'),
            Output('boxplot-turbulence', 'figure'),
            Output('error-by-mode', 'figure'),
            Output('boxplot-error', 'figure'),
            Output('scatter-plot', 'figure'),
            Output('geo-error', 'figure'),
            Output('temporal-error', 'figure'),
            Input('mode-selector', 'value'),
            Input('dropdown-error-time', 'value'),
            Input("error-metric-selector", 'value')
        )
        def update_graphs(mode, error_variable, error_metric):
            df = self.df.copy()

            # Global metrics
            mae = mean_absolute_error(df['takeoff_time'], df['prediction'])
            rmse = mean_squared_error(df['takeoff_time'], df['prediction'], squared=False)
            r2 = r2_score(df['takeoff_time'], df['prediction'])
            mape = self.mape

            fig_global = go.Figure()

            # MAE
            fig_global.add_trace(go.Indicator(
                mode="number+delta",
                value=mae,
                number={'valueformat': '.2f'},
                delta={'reference': self.mae_val, 'relative': False},
                title="MAE",
                domain={'x': [0, 0.25], 'y': [0, 1]}
            ))
            
            # RMSE
            fig_global.add_trace(go.Indicator(
                mode="number+delta",
                value=rmse,
                number={'valueformat': '.2f'},
                delta={'reference': self.rmse_val, 'relative': False},
                title="RMSE",
                domain={'x': [0.25, 0.5], 'y': [0, 1]}
            ))
            
            # R² Score
            fig_global.add_trace(go.Indicator(
                mode="number+delta",
                value=r2,
                number={'valueformat': '.2f'},
                title="R² Score",
                domain={'x': [0.5, 0.75], 'y': [0, 1]}
            ))
            
            # MAPE
            fig_global.add_trace(go.Indicator(
                mode="number+delta",
                value=mape,
                number={'valueformat': '.2f'},
                title="MAPE",
                domain={'x': [0.75, 1], 'y': [0, 1]}
            ))

            # Histograma de errores + Boxplot
            fig_hist = make_subplots(rows=2, cols=1, row_heights=[0.7, 0.3], shared_xaxes=True)
            fig_hist.add_trace(
                px.histogram(df, x='error').data[0],
                row=1, col=1
            )
            fig_hist.add_trace(
                px.box(df, x='error').data[0],
                row=2, col=1
            )
            fig_hist.update_layout(height=600)

            # Heatmap de error por día y hora

            heatmap_data = df.groupby(['weekday', 'hour']).agg(
                MAE=('abs_error', 'mean'),
                RMSE=('squared_error', lambda x: np.sqrt(np.mean(x)))
            ).reset_index()

            ordered_days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
            heatmap_data['weekday'] = pd.Categorical(heatmap_data['weekday'], categories=ordered_days, ordered=True)
            
            # Reordenar por día de la semana
            heatmap_data = heatmap_data.sort_values('weekday')
            
            # Crear el heatmap
            fig_heatmap = px.imshow(
                heatmap_data.pivot(index='weekday', columns='hour', values=error_metric),
                color_continuous_scale='Viridis',
                labels={'color': error_metric},
            )

            # Errores en función de la variable seleccionada
            fig_variable = px.scatter(df, x=error_variable, y='error',
                                      color='abs_error', color_continuous_scale='Viridis')

            # Boxplot por turbulencia y avión anterior
            df_melted = pd.melt(df, id_vars=['error'], value_vars=['turbulence_category', 'last_event_turb_cat'],
                    var_name='category_type', value_name='category_value')

            # Ahora creamos el gráfico, con 'category_value' en el eje x y 'category_type' como el factor
            fig_turbulence = px.box(df_melted, x='category_value', y='error', 
                                    color='category_type', 
                                    color_discrete_sequence=px.colors.qualitative.Set3)

            # MAE y RMSE por runway / holding_point
            group = df.groupby(mode).agg(
                MAE=('abs_error', 'mean'),
                RMSE=('squared_error', lambda x: np.sqrt(np.mean(x)))
            ).reset_index()
            fig_group = px.bar(group, x=mode, y=['MAE', 'RMSE'], barmode='group',
                               color_discrete_sequence=['#1f77b4', '#ff7f0e'])

            # Boxplot sin puntos
            fig_box = px.box(df, x=mode, y='error',
                             color_discrete_sequence=['#9467bd'])

            # Scatter plot
            fig_scatter = px.scatter(df, x='takeoff_time', y='prediction', color='abs_error',
                                     color_continuous_scale='Viridis')
            fig_scatter.add_trace(go.Scatter(
                x=[df['takeoff_time'].min(), df['takeoff_time'].max()],
                y=[df['takeoff_time'].min(), df['takeoff_time'].max()],
                mode='lines', name='Ideal', line=dict(dash='dash')
            ))

            # Mapa
            # Agrupamos los errores por cada punto de espera o pista
            if mode == 'runway':
                # Agrupar por pista
                df_grouped = df.groupby('runway').agg(
                    MAE=('abs_error', 'mean'),
                    RMSE=('squared_error', lambda x: np.sqrt(np.mean(x)))
                ).reset_index()

                # Merge con las coordenadas de las pistas
                df_geo = pd.merge(df_grouped, self.runways_df, on='runway')
                
                # Map
                fig_map = px.scatter_mapbox(
                    df_geo, lat="lat", lon="lon", color=error_metric, size=error_metric, 
                    hover_name="runway", color_continuous_scale="Viridis", 
                    size_max=20, zoom=12)
                fig_map.update_layout(mapbox_style="carto-positron", mapbox_center_lon=-3.567528, mapbox_center_lat=40.494597)
            
            elif mode == 'holding_point':
                # Agrupar por punto de espera
                df_grouped = df.groupby('holding_point').agg(
                    MAE=('abs_error', 'mean'),
                    RMSE=('squared_error', lambda x: np.sqrt(np.mean(x)))
                ).reset_index()

                # Merge con las coordenadas de los puntos de espera
                df_geo = pd.merge(df_grouped, self.holding_points_df, on='holding_point')

                # Map
                fig_map = px.scatter_mapbox(
                    df_geo, lat="lat", lon="lon", color=error_metric, size=error_metric, 
                    hover_name="holding_point", color_continuous_scale="Viridis", 
                    size_max=20, zoom=12)
                fig_map.update_layout(mapbox_style="carto-positron", mapbox_center_lon=-3.559, mapbox_center_lat=40.490)


            # Evolución temporal (MAE y RMSE diarios)
            temporal = df.groupby('date').agg(
                MAE=('abs_error', 'mean'),
                RMSE=('squared_error', lambda x: np.sqrt(np.mean(x))),
            ).reset_index()

            fig_time = go.Figure()

            # Línea de MAE
            fig_time.add_trace(go.Scatter(
                x=temporal['date'], y=temporal['MAE'],
                mode='lines+markers', name='MAE diario',
                line=dict(color='#1f77b4')
            ))

            # Línea de RMSE
            fig_time.add_trace(go.Scatter(
                x=temporal['date'], y=temporal['RMSE'],
                mode='lines+markers', name='RMSE diario',
                line=dict(color='#ff7f0e')
            ))


            return (fig_global, fig_hist, fig_heatmap, fig_variable, fig_turbulence,
                    fig_group, fig_box, fig_scatter, fig_map, fig_time)

    
        app.run_server(debug=True, port=8050)