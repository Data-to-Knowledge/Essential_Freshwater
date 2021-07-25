##### TRENDS VIEWER PROTOTYPE APPLICATION IN PLOTLY DASH #####
# Prototype by Phil Clunies-Ross 20th July 2021

# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

# Import packages as required for Plotly Dash App
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from numpy.lib.index_tricks import fill_diagonal
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])


# Import simplified trend dataset and split between observed and modelled datasets
trend_data = pd.read_excel("C:/Users/Philcro/OneDrive - Environment Canterbury/Documents/GitHub/Essential_Freshwater/water_quality/python/Indicator Results Script/GW-Results.xlsx", sheet_name="TrendResults", parse_dates=True)
observed_data = pd.read_excel("C:/Users/Philcro/OneDrive - Environment Canterbury/Documents/GitHub/Essential_Freshwater/water_quality/python/Indicator Results Script/GW-Results.xlsx", sheet_name="TrendData", parse_dates=True)
years = [5, 10, 15, 20, 25, 30]
refined_trends = trend_data[trend_data['TrendLength'].isin(years)]

# modelled_data = trend_data[(trend_data['Type'] == 'Modelled')].round(1)
# observed_data = trend_data[(trend_data['Type'] == 'Observation')]

# Lay out page and components using bootstrap components
app.layout = html.Div([
    dbc.Container([
        dbc.Jumbotron([
            html.H1("Groundwater Nitrate: Trends Explorer", className="display-5"),
            html.Hr(className="my-2"),
            html.P(
                "Visualising groundwater nitrate trends in Canterbury. Use the drop down menu to select a monitoring site.",
                className="lead"
            )
        ]),
        dbc.Row([
            dcc.Dropdown(
                id='site-choice',
                options=[{'label': i, 'value': i} for i in trend_data['Site'].unique()],
                value='N33/0200', style={'width': '100%'}
            ) 
        ]),
        dbc.Row([
            dcc.Graph(
                id='trend_graph', style={'width': '100%'}
            ),
        ])
    ])
])

#### First app callback to populate measurement drop-down options ####

@app.callback(
    Output('trend_graph', 'figure'),
    Input('site-choice', 'value'))

def update_graph(site_choice):

    # Refine data based on drop-down selection
    annual_site_trends = refined_trends[(refined_trends['Site'] == site_choice) & (refined_trends['DataFrequency'] == 'Annual')]
    annual_site_observations = observed_data[(observed_data['Site'] == site_choice) & (observed_data['Frequency'] == 'Annual')]

    # Draw first line chart for trend data. Label accordingly
    fig = px.line(
        annual_site_trends, 
        x=annual_site_trends['HydroYear'], 
        y='Value', 
        color='TrendLength', 
        line_group='TrendLength', 
        color_discrete_sequence=["lightskyblue", "deepskyblue", "dodgerblue", "dodgerblue", "blue", "navy"],
        labels={'TrendLength': 'Trend length (years)', 'HydroYear': 'Year', 'TrendCategory': 'Trend direction', 'Slope': 'Slope (mg NO3/yr)'},
        hover_data=['HydroYear', 'Value', 'TrendLength', 'TrendCategory', 'Slope']
        )
    
    # Draw second plot for observed values
    fig2 = px.scatter(
        site_observations, 
        x='HydroYear', 
        y='Value',
        color_discrete_sequence=["orange"]
        )

    # Use plotly.graph_objects to combine figures
    combined_fig = go.Figure(data = fig.data + fig2.data)

    # Modify layout and chart titles
    combined_fig.update_layout(
        template="plotly_white", 
        legend_title_text='Trend Length (years)', 
        title= f"Site ID: {site_choice}. Observed nitrate (markers) and projections (lines)"
        )

    # Update observed value markers
    combined_fig.update_traces(
        marker=dict(size=12,
        line=dict(width=2,
        color='DarkSlateGrey')),
        selector=dict(mode='markers')
        )
    
    # Update axis labels
    combined_fig.update_xaxes(title_text='Year')
    combined_fig.update_yaxes(title_text='mg/L nitrate nitrogen')

    return combined_fig

if __name__ == '__main__':
    app.run_server(debug=True)