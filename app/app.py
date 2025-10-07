import os
from databricks import sql
from databricks.sdk.core import Config
import pandas as pd
import numpy as np
from shapely.geometry import Polygon
import dash
from dash import dcc, html, Input, Output, State, callback_context, no_update
import plotly.express as px
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
from databricks.sdk.core import Config
import dash_leaflet as dl
from dash.dependencies import Input, Output
from dash_extensions.javascript import arrow_function
from dash_extensions.enrich import DashProxy, Input, Output, html
import flask
import json
import datetime as dt

# Set up the app
app = DashProxy(__name__)

# Check for environment variables but don't fail if they're not set (for development)
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
default_catalog = "timo"
default_schema = "cat_risk"
default_table = "cat_risk_scores_h3"
default_column = "h3_index"

catastrophe_types = [
    "flood_risk",
    "Avalanche_Risk",
    "Coastal_Flood_Risk",
    "Cold_Wave_Risk",
    "Drought_Risk",
    "Earthquake_Risk",
    "Hail_Risk",
    "Heat_Wave_Risk",
    "Hurricane_Risk",
    "Ice_Storm_Risk",
    "Landslide_Risk",
    "Lightning_Risk",
    "River_Flood_Risk",
    "Strong_Wind_Risk",
    "Tornado_Risk",
    "Tsunami_Risk",
    "Volcano_Risk",
    "Wildfire_Risk",
    "Winter_Weather_Risk"
]
default_catastrophe_type = "flood_risk"

global_center = None
global_zoom = None
global_bounds = None

# tile_layer_url = "http://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png"
tile_layer_url = "http://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png"

if not DATABRICKS_WAREHOUSE_ID:
    print("Warning: DATABRICKS_WAREHOUSE_ID not set. Cannot pull data.")

def get_databricks_token():
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

    if not DATABRICKS_TOKEN:
        print("DATABRICKS_TOKEN not set in environment variables, using on-behalf-of authentication.")
        DATABRICKS_TOKEN = flask.request.headers.get('X-Forwarded-Access-Token')
    return DATABRICKS_TOKEN

def get_databricks_server_hostname():
    DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_HOST")
    if not DATABRICKS_SERVER_HOSTNAME:
        print("DATABRICKS_SERVER_HOSTNAME not set in environment variables pulling from config.")
        cfg = Config()
        DATABRICKS_SERVER_HOSTNAME = cfg.host
    return DATABRICKS_SERVER_HOSTNAME

def sqlQuery(query: str) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    # print("RUNNING QUERY:", query)
    DATABRICKS_SERVER_HOSTNAME = get_databricks_server_hostname()
    DATABRICKS_TOKEN = get_databricks_token()
    with sql.connect(
        http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        access_token=DATABRICKS_TOKEN
    ) as connection:
        print("CONNECTION MADE")
        with connection.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=columns)
        return df

# Fetch the all h3 data
def get_data(catastrophe_type=None, resolution=9, bounds=None, column_resolution=None):
    stime = dt.datetime.now()
    
    catalog = "timo"
    schema = "cat_risk"
    table = "cat_risk_scores_h3"
    column = "h3_index"
    
    if not catalog or not schema or not table or not column:
        print("No catalog, schema, table, or column provided. Returning empty data.")
        return []
    
    try:
        bounds_wkt = bounds_to_wkt(bounds) if bounds else None
        resolution = min([int(column_resolution), resolution])
        print(f"RESOLUTION: {resolution}")
        print(f"RESOLUTION QUERY TOOK:    {dt.datetime.now() - stime}")
        stime = dt.datetime.now()

        query = f"""
                    WITH cell_agg AS (
                    SELECT
                        h3_toparent({column}, {resolution}) as h3_cell_id,
                        AVG({catastrophe_type}) as value
                        -- AVG(coalesce({catastrophe_type}, 0)) as value
                    FROM timo.cat_risk.cat_risk_scores_h3 -- {catalog}.{schema}.{table}
                    WHERE {f"h3_toparent({column}, {resolution}) IN (SELECT EXPLODE(H3_COVERASH3('{bounds_wkt}', {resolution})))" if bounds_wkt else "1=1"}
                    GROUP BY h3_cell_id
                    HAVING value > 0
                    )
                    SELECT h3_boundaryasgeojson(h3_cell_id) as hex_boundary,
                            value
                    FROM cell_agg
                    -- ORDER BY value DESC
        """
        print(query)
        data = sqlQuery(query)
        # print(data.head())
        # Convert any ndarray columns to lists
        for col in data.columns:
            if isinstance(data[col].iloc[0], np.ndarray):
                data[col] = data[col].apply(list)
    except Exception as e:
        print(f"An error occurred in querying data: {str(e)}")
        print("Returning empty data.")
        data = []
    
    print(f"DATA QUERY TOOK:    {dt.datetime.now() - stime}")
    # print(data.head())
    print("ROWS:", len(data))
    return data

def get_events():
    DATABRICKS_SERVER_HOSTNAME = get_databricks_server_hostname()
    DATABRICKS_TOKEN = get_databricks_token()
    with sql.connect(
        http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        access_token=DATABRICKS_TOKEN
    ) as connection:
        with connection.cursor() as cursor:
            query = f"""
                SELECT DISTINCT event_name
                FROM timo.cat_risk.cat_events
                ORDER BY event_name
            """
            cursor.execute(query)
            events = cursor.fetchall()
            events = [event.event_name for event in events]
            # print(catalogs)
        return events

def get_event_details(event_name):
    DATABRICKS_SERVER_HOSTNAME = get_databricks_server_hostname()
    DATABRICKS_TOKEN = get_databricks_token()
    with sql.connect(
        http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        access_token=DATABRICKS_TOKEN
    ) as connection:
        with connection.cursor() as cursor:
            query = f"""
                SELECT *
                FROM timo.cat_risk.cat_events
                WHERE event_name = '{event_name}'
            """
            cursor.execute(query)
            events = cursor.fetchall()
            event_details = [event.wkt for event in events][0]
            # print(catalogs)
        return event_details

def get_affected_portfolios(event_name=None):
    DATABRICKS_SERVER_HOSTNAME = get_databricks_server_hostname()
    DATABRICKS_TOKEN = get_databricks_token()
    with sql.connect(
        http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        access_token=DATABRICKS_TOKEN
    ) as connection:
        with connection.cursor() as cursor:
            event_wkt = get_event_details(event_name)
            query = f"""
                SELECT id, property_value, name, lat, lon, housenumber, street, city, state, postcode
                FROM timo.cat_risk.portfolio
                WHERE ST_CONTAINS(ST_GEOMFROMTEXT('{event_wkt}'), ST_POINT(lon, lat))
            """
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(results, columns=columns)
        return df

def style_function(value, deciles):
    fill_color = '#FFFFFF'  # default white
    
    if value >= deciles[9]:
        fill_color = '#800026'      # darkest red
    elif value >= deciles[8]:
        fill_color = '#A50026'      # very dark red
    elif value >= deciles[7]:
        fill_color = '#BD0026'      # dark red
    elif value >= deciles[6]:
        fill_color = '#E31A1C'      # red
    elif value >= deciles[5]:
        fill_color = '#FC4E2A'      # orange-red
    elif value >= deciles[4]:
        fill_color = '#FD8D3C'      # orange
    elif value >= deciles[3]:
        fill_color = '#FEB24C'      # light orange
    elif value >= deciles[2]:
        fill_color = '#FED976'      # yellow
    elif value >= deciles[1]:
        fill_color = '#FFEDA0'      # light yellow
    elif value >= deciles[0]:
        fill_color = '#FFFFCC'      # very light yellow
    
    return {
        "fillColor": fill_color,
        "weight": 1,
        "opacity": 0.9,
        "color": fill_color,
        "fillOpacity": 0.7
    };

def create_legend(deciles):
    """Create a legend component for the map"""
    legend_items = [
        {"color": "#FFFFCC", "label": f"< {deciles[1]:.1f}"},
        {"color": "#FFEDA0", "label": f"{deciles[1]:.1f}-{deciles[2]:.1f}"},
        {"color": "#FED976", "label": f"{deciles[2]:.1f}-{deciles[3]:.1f}"},
        {"color": "#FEB24C", "label": f"{deciles[3]:.1f}-{deciles[4]:.1f}"},
        {"color": "#FD8D3C", "label": f"{deciles[4]:.1f}-{deciles[5]:.1f}"},
        {"color": "#FC4E2A", "label": f"{deciles[5]:.1f}-{deciles[6]:.1f}"},
        {"color": "#E31A1C", "label": f"{deciles[6]:.1f}-{deciles[7]:.1f}"},
        {"color": "#BD0026", "label": f"{deciles[7]:.1f}-{deciles[8]:.1f}"},
        {"color": "#A50026", "label": f"{deciles[8]:.1f}-{deciles[9]:.1f}"},
        {"color": "#800026", "label": f"≥ {deciles[9]:.1f}"}
    ]
    
    legend_divs = []
    for item in legend_items:
        legend_divs.append(
            html.Div([
                html.Div(
                    style={
                        "backgroundColor": item["color"],
                        "width": "20px",
                        "height": "20px",
                        "border": "1px solid #000",
                        "display": "inline-block",
                        "marginRight": "8px"
                    }
                ),
                html.Span(item["label"], style={"fontSize": "12px", "color": "#FFFFFF"})
            ], style={"marginBottom": "5px"})
        )
    
    return html.Div([
        html.Div("Risk Score",
                 style={"marginBottom": "10px",
                        "fontSize": "14px",
                        "color": "#FFFFFF",
                        "fontFamily": "Helvetica",
                        "fontWeight": "bold"}),
        html.Div(legend_divs)
    ], 
        style={
        "position": "absolute",
        "top": "10px",
        "right": "10px",
        "backgroundColor": "#3A3A3A",
        "padding": "8px 16px", #"10px",
        "borderRadius": "4px",
        "boxShadow": "0 0 10px rgba(0,0,0,0.3)",
        "zIndex": "1000",
        "fontFamily": "Helvetica"
    }
    )

def zoom_to_h3_resolution(zoom):
    if zoom < 4:
        return 4
    elif zoom < 8:
        return 5
    elif zoom < 10:
        return 7
    elif zoom < 12:
        return 8
    elif zoom < 13:
        return 8
    elif zoom < 14:
        return 8
    elif zoom < 16:
        return 8
    elif zoom < 17:
        return 8
    else:
        return 8
    
def bounds_to_wkt(bounds):
    # Input bounds: [southwest, northeast] in (lat, lon)
    sw = bounds[0]  # [lat, lon]
    ne = bounds[1]  # [lat, lon]

    # Create polygon corners: lon/lat order
    polygon_coords = [
        (sw[1], sw[0]),  # lower left
        (sw[1], ne[0]),  # upper left
        (ne[1], ne[0]),  # upper right
        (ne[1], sw[0]),  # lower right
        (sw[1], sw[0])   # close polygon
    ]

    # Create polygon and convert to WKT
    poly = Polygon(polygon_coords)
    wkt_string = poly.wkt
    return wkt_string

def create_linear_color_scale(data, n_colors=10):
    """Create linear-scaled color breaks for mapping"""
    data_positive = data[data > 0]
    
    min_val = 0.0 #data_positive.min()
    max_val = 1.0 #data_positive.max()
    
    # Equal intervals in linear space
    breaks = np.linspace(min_val, max_val, n_colors + 1)
    
    return breaks 

def wkt_to_bounds(wkt_string):
    """Convert WKT polygon to bounds for leaflet flyTo"""
    try:
        from shapely import wkt
        polygon = wkt.loads(wkt_string)
        bounds = polygon.bounds
        flyto_bounds = [
            [bounds[1], bounds[0]],  # [miny, minx]
            [bounds[3], bounds[2]]   # [maxy, maxx]
        ]
        return flyto_bounds
    except Exception as e:
        print(f"Error converting WKT to bounds: {e}")
        return None

def wkt_to_geojson(wkt_string):
    """
    Convert a WKT string to a valid GeoJSON geometry dict.
    """
    try:
        from shapely import wkt as shapely_wkt
        from shapely.geometry import mapping
        geom = shapely_wkt.loads(wkt_string)
        geojson_geom = mapping(geom)
        return geojson_geom
    except Exception as e:
        print(f"Error converting WKT to GeoJSON: {e}")
        return None

def polygon_clip_to_bounds(polygon_coords, bounds):
    """
    Clips a polygon (list of [lat, lon]) to a bounding box (list: [[min_lat, min_lon], [max_lat, max_lon]])
    Returns a new list of [lat, lon] coordinates for the clipped polygon.
    """
    from shapely.geometry import Polygon, box, mapping

    # Convert input to (lon, lat) for Shapely
    poly_xy = [(coord[1], coord[0]) for coord in polygon_coords]
    poly = Polygon(poly_xy)

    # Extract bounds
    min_lat, min_lon = bounds[0]
    max_lat, max_lon = bounds[1]
    bbox = box(min_lon, min_lat, max_lon, max_lat)

    # Perform intersection
    clipped = poly.intersection(bbox)

    # If the result is empty, return empty list
    if clipped.is_empty:
        return []

    # If the result is a MultiPolygon, take all exterior coords
    if clipped.geom_type == 'MultiPolygon':
        result = []
        for part in clipped.geoms:
            coords = [[lat, lon] for lon, lat in list(part.exterior.coords)]
            result.append(coords)
        # Flatten if only one polygon, else return list of polygons
        if len(result) == 1:
            return result[0]
        return result
    elif clipped.geom_type == 'Polygon':
        # Return exterior coordinates as [[lat, lon], ...]
        return [[lat, lon] for lon, lat in list(clipped.exterior.coords)]
    else:
        # Not a polygon, return empty
        return []


deciles = create_linear_color_scale(np.linspace(0.0, 1.0, 11))
legend = create_legend(deciles)

def data_to_polygons(map_data):
    hex_centers_lats = []
    hex_centers_lngs = []
    hex_boundaries_polygons = []
    hex_boundaries_geojson = {
        "type": "FeatureCollection",
        "features": []
    }
    dlPolygons = []
    for i in range(len(map_data)):
        hex_boundaries_polygon = [[coord[1], coord[0]] for coord in json.loads(map_data.iloc[i]['hex_boundary'])['coordinates'][0]]
        hex_boundaries_polygons.append(hex_boundaries_polygon)
        
        hex_boundary_element = {'type': 'Feature'}
        hex_boundary_element['geometry'] = json.loads(map_data.iloc[i]['hex_boundary'])
        hex_centers_lats.append(json.loads(map_data.iloc[i]['hex_boundary'])['coordinates'][0][0][1])
        hex_centers_lngs.append(json.loads(map_data.iloc[i]['hex_boundary'])['coordinates'][0][0][0])
        hex_boundary_element['properties'] = {
            'value': map_data.iloc[i]['value'].item(),
            'color': style_function(map_data.iloc[i]['value'].item(), deciles)
        }
        hex_boundaries_geojson['features'].append(hex_boundary_element)

        style = style_function(map_data.iloc[i]['value'].item(), deciles)

        dlPolygons.append(dl.Polygon(
            positions=hex_boundaries_polygon,
            fillColor=style['fillColor'],
            color=style['color'],
            weight=style['weight'],
            opacity=style['opacity'],
            fillOpacity=style['fillOpacity']
            )
        )
    return dlPolygons

    # polygons = []
    # for i in range(len(map_data)):
    #     polygons.append(dl.Polygon(positions=map_data.iloc[i]['hex_boundary'], color="#39FF14", fillOpacity=0.04))
    # return polygons

app.layout = html.Div(
    [
        dcc.Location(id="url"),
        dcc.Store(id='portfolio-data-store', data=[]),
        dcc.Store(id='highlighted-portfolio', data=None),
        dcc.Store(id='clicked-portfolio', data=None),
        # Add dropdown selection controls at the top
        html.Div(
            [
                html.Div(
                    [
                        html.Label("Catastrophe Type:", style={"color": "#FFFFFF", "fontFamily": "Helvetica", "fontWeight": "bold", "marginRight": "10px"}),
                        dcc.Dropdown(
                            id="catastrophe-dropdown",
                            placeholder="Loading default...",
                            style={"width": "200px", "backgroundColor": "#FFFFFF", "fontFamily": "Helvetica", "color": "#3A3A3A"},
                            options=catastrophe_types,
                            value=default_catastrophe_type
                        )
                    ],
                    style={"display": "inline-block", "marginRight": "20px"}
                ),
                html.Div(
                    [
                        html.Label("Event:", style={"color": "#FFFFFF", "fontFamily": "Helvetica","fontWeight": "bold", "marginRight": "10px"}),
                        dcc.Dropdown(
                            id="event-dropdown",
                            placeholder="Select an event...",
                            disabled=False,
                            style={"width": "200px", "backgroundColor": "#FFFFFF", "fontFamily": "Helvetica", "color": "#3A3A3A"},
                        )
                    ],
                    style={"display": "inline-block", "marginRight": "20px"}
                ),
                # html.Div(
                #     [
                #         html.Label("Table:", style={"color": "#FFFFFF", "fontFamily": "Helvetica", "fontWeight": "bold", "marginRight": "10px"}),
                #         dcc.Dropdown(
                #             id="table-dropdown",
                #             placeholder="Loading default...",
                #             disabled=True,
                #             style={"width": "200px", "backgroundColor": "#FFFFFF", "fontFamily": "Helvetica", "color": "#3A3A3A"},
                #         )
                #     ],
                #     style={"display": "inline-block", "marginRight": "20px"}
                # ),
                # html.Div(
                #     [
                #         html.Label("Column:", style={"color": "#FFFFFF", "fontFamily": "Helvetica", "fontWeight": "bold", "marginRight": "10px"}),
                #         dcc.Dropdown(
                #             id="column-dropdown",
                #             placeholder="Loading default...",
                #             disabled=True,
                #             style={"width": "200px", "backgroundColor": "#FFFFFF", "fontFamily": "Helvetica", "color": "#3A3A3A"},
                #         ),
                #     ],
                #     style={"display": "inline-block", "marginRight": "20px"}
                # ),
                html.Div(
                    [                        
                        dcc.Loading(
                            id="loading-spinner",
                            type="circle",  # options: "default", "circle", "dot", "cube"
                            children=html.P(
                                            id="column-description",
                                            style={
                                                "color": "#FFFFFF", 
                                                "fontFamily": "Helvetica", 
                                                "fontSize": "14px",
                                                "margin": "0",
                                                "display": "inline-block",
                                                "alignItems": "stretch",
                                                "verticalAlign": "middle"
                                            }
                                        ),
                            color="#FFFFFF",
                            style={"transform": "scale(0.5)"},
                        ),
                    ],
                    style={"display": "inline-block", "marginRight": "20px", "alignItems": "stretch"}
                ),
            ],
            style={
                "backgroundColor": "#3A3A3A",
                "padding": "15px",
                "borderRadius": "8px",
                # "marginBottom": "20px",
                "boxShadow": "0 2px 4px rgba(0,0,0,0.3)"
            }
        ),
        # Two column layout for map and portfolio list
        html.Div(
            [
                # Left column - Map (75%)
                html.Div(
                    [
                        # Refresh button positioned above map's upper right corner
                        html.Div(
                            [
                                dbc.Button(
                                    "Refresh Map",
                                    id="refresh-button",
                                    className="refresh-button",
                                    disabled=False,
                                    style={
                                        "backgroundColor": "#3A3A3A",
                                        "color": "#FFFFFF",
                                        "cursor": "pointer",
                                        "borderRadius": "4px",
                                        "fontSize": "14px",
                                        "fontFamily": "Helvetica",
                                        "fontWeight": "bold",
                                        "width": "150px",
                                        "height": "35px",
                                        "border": "1px solid #FFFFFF",
                                    }
                                )
                            ],
                            style={
                                "position": "absolute",
                                "top": "-50px",
                                "right": "10px",
                                "zIndex": "1000"
                            }
                        ),
                        dcc.Loading(
                            children=[
                                dl.Map(
                                    [
                                        dl.TileLayer(url=tile_layer_url, attribution='© Mapbox © OpenStreetMap'),
                                        dl.LayerGroup(id="map-polygons", children=[]) #dl.Polygon(color='#39FF14', positions=[[25.2, -81.6], [25.2, -79.8], [26.6, -79.8], [26.6, -81.6]])])
                                        # dl.Polygon(color='#39FF14', positions=[[25.2, -81.6], [25.2, -79.8], [26.6, -79.8], [26.6, -81.6]])
                                    ],
                                    center=[39.8283, -98.5795],
                                    zoom=5,
                                    trackViewport=True,
                                    zoomControl=False,
                                    # bounds=bounds,
                                    style={"width": "100%", "height": "100vh"},
                                    id="map"
                                ),
                                # html.Div(id="map-container", children=leaflet_map),
                                html.Div(id="legend-container", children=legend),
                                # Hidden div to trigger marker highlighting
                                html.Div(id="marker-highlight-trigger", style={"display": "none"})
                            ],
                            id='loading-map',
                            type='default',
                            color='#3A3A3A',
                            overlay_style={"visibility":"visible", "filter": "blur(3px)"},
                        ),
                    ],
                    style={"width": "75%", "display": "inline-block", "verticalAlign": "top", "position": "relative"}
                ),
                # Right column - Portfolio List (25%)
                html.Div(
                    [
                        html.Div(
                            [
                                html.H4("Affected Portfolios", 
                                       style={
                                           "color": "#FFFFFF", 
                                           "fontFamily": "Helvetica", 
                                           "fontWeight": "bold",
                                           "marginBottom": "15px",
                                           "textAlign": "center"
                                       }),
                                html.Div(id="portfolio-count",
                                        style={
                                            "color": "#CCCCCC",
                                            "fontFamily": "Helvetica",
                                            "fontSize": "12px",
                                            "marginBottom": "10px",
                                            "textAlign": "center"
                                        }),
                                dcc.Loading(
                                    id="loading-portfolios",
                                    type="circle",
                                    children=html.Div(
                                        id="portfolio-list",
                                        style={
                                            "maxHeight": "calc(100vh - 150px)",
                                            "overflowY": "auto",
                                            "overflowX": "hidden"
                                        }
                                    ),
                                    color="#FFFFFF"
                                )
                            ],
                            style={
                                "backgroundColor": "#3A3A3A",
                                "padding": "15px",
                                "borderRadius": "8px",
                                "boxShadow": "0 2px 4px rgba(0,0,0,0.3)",
                                "height": "100vh"
                            }
                        )
                    ],
                    style={"width": "25%", "display": "inline-block", "verticalAlign": "top"}
                ),
            ],
            style={"display": "flex", "width": "100%"}
        ),
    ],
    style={"backgroundColor": "#29323C"},
    
)

# @app.callback(
#     [Output('map', 'children'),
#      Output('legend-container', 'children'),
#      Output('refresh-button', 'style')],
#     Input('refresh-button', 'n_clicks'),
#     [State("map", "center"),
#      State("map", "zoom"),
#      State("map", "bounds"),
#      State("catastrophe-dropdown", "value"),
#      State("event-dropdown", "value")
#      ],
#      prevent_initial_call=True
# )




# Add callback for refresh button and initial load
@app.callback(
    [Output('map-polygons', 'children', allow_duplicate=True),
     Output('refresh-button', 'style')],
    Input('refresh-button', 'n_clicks'),
    [State("map", "center"),
     State("map", "zoom"),
     State("map", "bounds"),
     State("catastrophe-dropdown", "value"),
     State("event-dropdown", "value"),
     State('map-polygons', 'children'),
     ],
     prevent_initial_call=True
)
def update_map(n_clicks, center, zoom, bounds, catastrophe_type, event_name, children):
    # Define button styles
    active_style = {
        "backgroundColor": "#3A3A3A",
        "color": "#FFFFFF",
        "cursor": "pointer",
        "borderRadius": "4px",
        "fontSize": "14px",
        "fontFamily": "Helvetica",
        "fontWeight": "bold",
        "width": "150px",
        "height": "35px",
        "border": "1px solid #FFFFFF"
    }
    
    if n_clicks is None:
        # Initial load - return the pre-created map and legend with active button style
        print("Initial map load")
        polygons = []
        return polygons, active_style #leaflet_map, legend, active_style
    else:
        # Refresh button clicked
        print(f"Refreshing map and data (click #{n_clicks})")
        global global_center
        global global_zoom
        global global_bounds

        # Update global variables
        global_center = center if center is not None else global_center
        global_zoom = zoom if zoom is not None else global_zoom
        global_bounds = bounds if bounds is not None else global_bounds

        if isinstance(global_center, list):
            global_center = {'lat': global_center[0], 'lng': global_center[1]}
        print(f"Global Center: {global_center}, Global Zoom: {global_zoom}, Global Bounds: {global_bounds}")

        resolution = zoom_to_h3_resolution(global_zoom)

        # Fetch new data
        new_map_data = get_data(catastrophe_type=catastrophe_type, bounds=global_bounds, resolution=resolution, column_resolution=resolution)
        
        new_polygons = data_to_polygons(new_map_data)

        # Keep event polygon in children
        filtered_children = [x for x in children if 'id' in x['props'] and x["props"]["id"] == 'event-polygon']
        polygons = new_polygons + filtered_children

        # event_polygon = []
        # if event_name:
        #     event_wkt = get_event_details(event_name)
        #     event_polygon = [list([coord[1], coord[0]]) for coord in wkt_to_geojson(event_wkt)['coordinates'][0]][:-1]
        #     # print(f"Event Polygon: {event_polygon}")
        #     # print(f"Global Bounds: {global_bounds}")
        #     event_polygon = polygon_clip_to_bounds(event_polygon, global_bounds)
        #     # print(f"Event Polygon Clipped to Bounds: {event_polygon}")
        #     event_polygon = [dl.Polygon(positions=event_polygon, color="#39FF14", fillOpacity=0.04)]
            
        # polygons = polygons + new_polygons + event_polygon
        # print(f"Polygons: {polygons}")
        # Create new map and legend
        # new_leaflet_map, new_legend = create_leaflet_map(new_map_data, zoom=global_zoom, center=global_center, wkt=event_wkt)
        
        print("Map refreshed successfully!")
        return polygons, active_style


# Callback to immediately update button style when clicked (before map update)
@app.callback(
    Output('refresh-button', 'style', allow_duplicate=True),
    Input('refresh-button', 'n_clicks'),
    prevent_initial_call=True
)
def update_button_style_on_click(n_clicks):
    """Update button style immediately when clicked to show inactive state during loading"""
    if n_clicks is not None:
        # Button was clicked - show inactive/loading state
        inactive_style = {
            "backgroundColor": "#666666",
            "color": "#CCCCCC",
            "cursor": "not-allowed",
            "borderRadius": "4px",
            "fontSize": "14px",
            "fontFamily": "Helvetica",
            "fontWeight": "bold",
            "width": "150px",
            "height": "35px",
            "border": "1px solid #999999",
            "opacity": "0.6"
        }
        return inactive_style
    
    # Default active style (shouldn't reach here due to prevent_initial_call=True)
    return {
        "backgroundColor": "#3A3A3A",
        "color": "#FFFFFF",
        "cursor": "pointer",
        "borderRadius": "4px",
        "fontSize": "14px",
        "fontFamily": "Helvetica",
        "fontWeight": "bold",
        "width": "150px",
        "height": "35px",
        "border": "1px solid #FFFFFF"
    }

# Callback to populate catalog dropdown on app load
@app.callback(
    [Output('event-dropdown', 'options'),
     Output('event-dropdown', 'placeholder'),
     Output('event-dropdown', 'disabled'),
     Output('event-dropdown', 'value')],
    [Input('event-dropdown', 'id'),
     Input("url", "pathname")],
    prevent_initial_call=False
)
def populate_events(trigger, pathname):
    """Populate the catalog dropdown with available catalogs"""
    # print("Populating catalogs dropdown...")
    
    try:
        print("Fetching events from database...")
        events = get_events()
        print("Events fetched successfully")
        return events, "Select an event...", False, None
        
    except Exception as e:
        print(f"Error fetching events: {e}")
        print("Returning empty list")
        return [], "Select an event...", False, None

# Callback to fly to event and add event polygon to map
@app.callback(
    Output('map', 'viewport'),
    [Input('event-dropdown', 'value')],
    prevent_initial_call=True,
)
def flyto_event(event_name):
    """Flyto the event bounds"""

    global global_bounds
    print(f"intial event_name: {event_name}")

    if event_name is not None:
        event_wkt = get_event_details(event_name)
        flyto_bounds = wkt_to_bounds(event_wkt)
        global_bounds = flyto_bounds

        return dict(
            bounds=global_bounds,
            transition="flyTo",
            duration=3000
        )
    else:
        return dict(bounds=global_bounds)

# Callback to update both portfolio list and markers when event is selected
@app.callback(
    [Output('portfolio-list', 'children'),
     Output('portfolio-count', 'children'),
     Output('portfolio-data-store', 'data'),
     Output('map-polygons', 'children')],
    [Input('event-dropdown', 'value')],
    [State('map-polygons', 'children')],
    prevent_initial_call=True
)
def update_portfolio_list_and_markers(event_name, children):
    """Update the portfolio list and map markers when an event is selected"""
    if event_name is None:
        return html.Div(
            "Select an event to view affected portfolios",
            style={
                "color": "#CCCCCC",
                "fontFamily": "Helvetica",
                "fontSize": "14px",
                "textAlign": "center",
                "padding": "20px"
            }
        ), "", [], children
    
    try:
        # Fetch portfolios affected by the event
        portfolios_df = get_affected_portfolios(event_name)
        
        if portfolios_df.empty:
            return html.Div(
                "No portfolios found in this event area",
                style={
                    "color": "#CCCCCC",
                    "fontFamily": "Helvetica",
                    "fontSize": "14px",
                    "textAlign": "center",
                    "padding": "20px"
                }
            ), "0 portfolios", [], children
        
        # Store portfolio data for markers
        portfolio_data = portfolios_df.to_dict('records')
        
        # Create portfolio cards with hover interaction
        portfolio_cards = []
        for idx, row in portfolios_df.iterrows():
            card = html.Button(
                [
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Strong("ID: ", style={"color": "#FFFFFF"}),
                                    html.Span(str(row['id']), style={"color": "#CCCCCC"})
                                ],
                                style={"marginBottom": "5px"}
                            ),
                            html.Div(
                                [
                                    html.Strong("Property Value: ", style={"color": "#FFFFFF"}),
                                    html.Span(f"${row['property_value']:,.2f}" if pd.notna(row['property_value']) else "N/A", 
                                             style={"color": "#4CAF50", "fontWeight": "bold"})
                                ],
                                style={"marginBottom": "5px"}
                            ),
                            html.Div(
                                [
                                    html.Strong("Name: ", style={"color": "#FFFFFF"}),
                                    html.Span(str(row['name']) if pd.notna(row['name']) else "N/A", 
                                             style={"color": "#CCCCCC"})
                                ],
                                style={"marginBottom": "5px"}
                            ),
                            html.Div(
                                [
                                    html.Strong("Address: ", style={"color": "#FFFFFF"}),
                                    html.Span(
                                        f"{row['housenumber'] if pd.notna(row['housenumber']) else ''} "
                                        f"{row['street'] if pd.notna(row['street']) else ''} "
                                        f"{row['city'] if pd.notna(row['city']) else ''}, "
                                        f"{row['state'] if pd.notna(row['state']) else ''} "
                                        f"{row['postcode'] if pd.notna(row['postcode']) else ''}",
                                        style={"color": "#CCCCCC"}
                                    )
                                ],
                                style={"marginBottom": "5px"}
                            ),
                            html.Div(
                                [
                                    html.Strong("Coordinates: ", style={"color": "#FFFFFF"}),
                                    html.Span(
                                        f"({row['lat']:.4f}, {row['lon']:.4f})",
                                        style={"color": "#CCCCCC", "fontSize": "11px"}
                                    )
                                ],
                                style={"marginBottom": "0px"}
                            ),
                        ]
                    )
                ],
                id={"type": "portfolio-card", "index": str(row['id'])},
                n_clicks=0,
                className="portfolio-card",
                style={
                    "backgroundColor": "#2C2C2C",
                    "padding": "12px",
                    "marginBottom": "10px",
                    "borderRadius": "6px",
                    "border": "1px solid #4A4A4A",
                    "fontFamily": "Helvetica",
                    "fontSize": "12px",
                    "cursor": "pointer",
                    "transition": "all 0.3s ease",
                    "width": "100%",
                    "textAlign": "left"
                }
            )
            portfolio_cards.append(card)
        
        count_text = f"{len(portfolios_df):,} portfolio{'s' if len(portfolios_df) != 1 else ''} affected"
        
        # Get event polygon to add to map
        event_wkt = get_event_details(event_name)
        event_polygon = [list([coord[1], coord[0]]) for coord in wkt_to_geojson(event_wkt)['coordinates'][0]][:-1]
        
        # Create markers for portfolios
        # Filter out any existing portfolio markers and event polygon
        filtered_children = [x for x in children 
                           if 'id' not in x.get('props', {}) 
                           or (not str(x['props']['id']).startswith('portfolio-marker-') 
                               and x['props']['id'] != 'event-polygon')]
        
        # Add event polygon
        event_polygon_element = dl.Polygon(id="event-polygon", positions=event_polygon, color="#39FF14", fillOpacity=0.05)
        
        # Add new portfolio markers with CircleMarker for better interactivity
        portfolio_markers = []
        for idx, row in portfolios_df.iterrows():
            # Create a marker with a unique ID
            marker = dl.CircleMarker(
                id={"type": "portfolio-marker", "index": str(row['id'])},
                center=[row['lat'], row['lon']],
                radius=8,
                color="#FF0000",
                fillColor="#FF4444",
                fillOpacity=0.8,
                weight=2,
                n_clicks=0,
                children=[
                    dl.Tooltip(
                        children=html.Div([
                            html.Strong(f"ID: {row['id']}", style={"display": "block", "marginBottom": "5px"}),
                            html.Span(f"Value: ${row['property_value']:,.2f}" if pd.notna(row['property_value']) else "Value: N/A", 
                                     style={"display": "block", "marginBottom": "3px"}),
                            html.Span(str(row['name']) if pd.notna(row['name']) else "", 
                                     style={"display": "block", "fontSize": "11px"})
                        ])
                    )
                ],
                className="portfolio-marker"
            )
            portfolio_markers.append(marker)
        
        # Combine existing elements with event polygon and new markers
        updated_children = filtered_children + [event_polygon_element] + portfolio_markers
        
        return html.Div(portfolio_cards), count_text, portfolio_data, updated_children
        
    except Exception as e:
        print(f"Error fetching portfolios: {e}")
        return html.Div(
            f"Error loading portfolios: {str(e)}",
            style={
                "color": "#FF6B6B",
                "fontFamily": "Helvetica",
                "fontSize": "14px",
                "textAlign": "center",
                "padding": "20px"
            }
        ), "Error", [], children
        
    # except Exception as e:
    #     print(f"Error fetching events: {e}")
    #     print("Returning empty list")
    #     return [], "Select an event...", False, None

# # Callback to populate table dropdown when schema is selected
# @app.callback(
#     [Output('table-dropdown', 'options'),
#      Output('table-dropdown', 'placeholder'),
#      Output('table-dropdown', 'disabled'),
#      Output('table-dropdown', 'value')],
#     Input('schema-dropdown', 'value'),
#     State('catalog-dropdown', 'value'),
#     prevent_initial_call=False
# )
# def populate_tables(selected_schema, selected_catalog):
#     """Populate the table dropdown when a schema is selected"""
#     # print(f"Populating tables for schema: {selected_schema}, catalog: {selected_catalog}")
    
#     global load_defaults
#     global default_table

#     if (not selected_schema or not selected_catalog) and default_table is None:
#         return [], "Select a table...", True, None
    
#     try:
#         print(f"Fetching tables for schema {selected_catalog}.{selected_schema}...")
#         tables = get_tables(selected_catalog, selected_schema)
#         print(f"Retrieved tables: {tables}")
#         return tables, "Select a table...", False, default_table if load_defaults else None
#     except Exception as e:
#         print(f"Error fetching tables for schema {selected_catalog}.{selected_schema}: {e}")
#         print("ASSUMING NOT AVAILABLE")
#         return [{'label': 'NOT AVAILABLE', 'value': 'NOT AVAILABLE'}], "Select a table...", False, None

# # Callback to populate column dropdown when table is selected
# @app.callback(
#     [Output('column-dropdown', 'options'),
#      Output('column-dropdown', 'placeholder'),
#      Output('column-dropdown', 'disabled'),
#      Output('column-dropdown', 'value')],
#     Input('table-dropdown', 'value'),
#     [State('catalog-dropdown', 'value'),
#      State('schema-dropdown', 'value')],
#     prevent_initial_call=False
# )
# def populate_columns(selected_table, selected_catalog, selected_schema):
#     """Populate the column dropdown when a table is selected"""
#     # print(f"Populating columns for table: {selected_table}, schema: {selected_schema}, catalog: {selected_catalog}")

#     global load_defaults
#     global default_column

#     if (not selected_table or not selected_catalog or not selected_schema) and default_column is None:
#         return [], "Select a column...", True, None
    
#     try:
#         print(f"Fetching columns for table {selected_catalog}.{selected_schema}.{selected_table}...")
#         columns = get_columns(selected_catalog, selected_schema, selected_table)
#         print(f"Retrieved columns: {columns}")
#         return columns, "Select a column...", False, default_column if load_defaults else None
#     except Exception as e:
#         print(f"Error fetching columns for table {selected_catalog}.{selected_schema}.{selected_table}: {e}")
#         print("ASSUMING NOT AVAILABLE")
#         return ['NOT AVAILABLE'], "Select a column...", False, None

# # Callback to validate column and show description
# @app.callback(
#     [Output('column-description', 'children'),
#      Output('refresh-button', 'disabled'),
#      Output('refresh-button', 'style'),
#      Output('refresh-button', 'title')],
#     [Input('column-dropdown', 'value'),
#      Input('catalog-dropdown', 'value'),
#      Input('schema-dropdown', 'value'),
#      Input('table-dropdown', 'value')],
#     prevent_initial_call=False
# )
# def validate_column(selected_column, selected_catalog, selected_schema, selected_table):
#     """Validate the selected column and return description"""
#     disabled_style = {
#         "backgroundColor": "#666666",
#         "color": "#CCCCCC",
#         "cursor": "not-allowed",
#         "borderRadius": "4px",
#         "fontSize": "14px",
#         "fontFamily": "Helvetica",
#         "fontWeight": "bold",
#         "width": "150px",
#         "height": "35px",
#         "border": "1px solid #999999",
#         "marginTop": "20px",
#         "opacity": "0.6"
#     }
#     enabled_style = {
#         "backgroundColor": "#3A3A3A",
#         "color": "#FFFFFF",
#         "cursor": "pointer",
#         "borderRadius": "4px",
#         "fontSize": "14px",
#         "fontFamily": "Helvetica",
#         "fontWeight": "bold",
#         "width": "150px",
#         "height": "35px",
#         "border": "1px solid #FFFFFF",
#         "marginTop": "20px"
#     }
    
#     if not selected_column or not selected_catalog or not selected_schema or not selected_table:
#         return "", True, disabled_style, "Select a valid H3 column"
    
#     print(f"Validating column: {selected_column}, table: {selected_table}, schema: {selected_schema}, catalog: {selected_catalog}")

#     try:
#         resolution_query = f"SELECT h3_resolution({selected_column}) as resolution FROM {selected_catalog}.{selected_schema}.{selected_table} LIMIT 1"
#         column_resolution = sqlQuery(resolution_query)['resolution'].iloc[0]
#         # print(f"Column resolution: {column_resolution}")

#         count_query = f"SELECT COUNT(*) as count FROM {selected_catalog}.{selected_schema}.{selected_table} WHERE {selected_column} IS NOT NULL"
#         count_result = sqlQuery(count_query)['count'].iloc[0]
#         # print(f"Count result: {count_result}")

#         if column_resolution is None or column_resolution == 0 or count_result == 0:
#             print("Column resolution is None or count is 0.")
#             return "Column is not valid H3", True, disabled_style, "Must select a valid H3 column"
#         else:
#             print("Column resolution is valid. Returning columns.")
#             return f"Column resolution: {column_resolution}; Row count: {format(count_result, ',')}", False, enabled_style, "Refresh map"
#     except Exception as e:
#         print(f"Column is not valid H3: {e}")
#         return "Column is not valid H3", True, disabled_style, "Must select a valid H3 column"

# Callback to handle clicks on portfolio cards or markers and update clicked-portfolio store
@app.callback(
    Output('clicked-portfolio', 'data'),
    [Input({"type": "portfolio-card", "index": dash.dependencies.ALL}, "n_clicks"),
     Input({"type": "portfolio-marker", "index": dash.dependencies.ALL}, "n_clicks")],
    [State({"type": "portfolio-card", "index": dash.dependencies.ALL}, "id"),
     State({"type": "portfolio-marker", "index": dash.dependencies.ALL}, "id"),
     State('clicked-portfolio', 'data')],
    prevent_initial_call=True
)
def handle_portfolio_click(card_clicks, marker_clicks, card_ids, marker_ids, current_clicked):
    """Handle clicks on portfolio cards or markers - only updates highlighting, no data reload"""
    ctx = callback_context
    
    if not ctx.triggered:
        return no_update
    
    triggered_id = ctx.triggered[0]['prop_id']
    
    # Parse which element was clicked
    if 'portfolio-card' in triggered_id:
        # A card was clicked - extract the portfolio ID from the triggered component
        try:
            triggered_dict = json.loads(triggered_id.split('.')[0])
            portfolio_id = triggered_dict['index']
            # Toggle off if clicking the same card
            if current_clicked == portfolio_id:
                return None
            return portfolio_id
        except:
            return no_update
    
    elif 'portfolio-marker' in triggered_id:
        # A marker was clicked - extract the portfolio ID from the triggered component
        try:
            triggered_dict = json.loads(triggered_id.split('.')[0])
            portfolio_id = triggered_dict['index']
            # Toggle off if clicking the same marker
            if current_clicked == portfolio_id:
                return None
            return portfolio_id
        except:
            return no_update
    
    return no_update

# Callback to update portfolio card styles based on clicked portfolio
# This only updates visual styling - NO data reload
@app.callback(
    Output({"type": "portfolio-card", "index": dash.dependencies.MATCH}, "style"),
    [Input('clicked-portfolio', 'data')],
    [State({"type": "portfolio-card", "index": dash.dependencies.MATCH}, "id")],
    prevent_initial_call=False
)
def update_card_style(clicked_portfolio, card_id):
    """Update card style based on whether it's clicked - visual update only, no data reload"""
    portfolio_id = card_id['index']
    
    base_style = {
        "backgroundColor": "#2C2C2C",
        "padding": "12px",
        "marginBottom": "10px",
        "borderRadius": "6px",
        "border": "1px solid #4A4A4A",
        "fontFamily": "Helvetica",
        "fontSize": "12px",
        "cursor": "pointer",
        "transition": "all 0.3s ease",
        "width": "100%",
        "textAlign": "left"
    }
    
    if clicked_portfolio == portfolio_id:
        # Highlighted style when clicked
        return {
            **base_style,
            "backgroundColor": "#3D3D3D",
            "border": "2px solid #4CAF50",
            "boxShadow": "0 4px 8px rgba(76, 175, 80, 0.3)",
            "transform": "translateX(5px)"
        }
    
    return base_style

# Callback to update marker styles based on clicked portfolio
# This only updates visual styling - NO data reload
@app.callback(
    [Output({"type": "portfolio-marker", "index": dash.dependencies.MATCH}, "color"),
     Output({"type": "portfolio-marker", "index": dash.dependencies.MATCH}, "fillColor"),
     Output({"type": "portfolio-marker", "index": dash.dependencies.MATCH}, "fillOpacity"),
     Output({"type": "portfolio-marker", "index": dash.dependencies.MATCH}, "weight"),
     Output({"type": "portfolio-marker", "index": dash.dependencies.MATCH}, "radius")],
    [Input('clicked-portfolio', 'data')],
    [State({"type": "portfolio-marker", "index": dash.dependencies.MATCH}, "id")],
    prevent_initial_call=False
)
def update_marker_style(clicked_portfolio, marker_id):
    """Update marker style based on whether it's clicked - visual update only, no data reload"""
    portfolio_id = marker_id['index']
    
    if clicked_portfolio == portfolio_id:
        # Highlighted style when clicked - green and larger
        return "#4CAF50", "#4CAF50", 1.0, 4, 10
    
    # Default style - red
    return "#FF0000", "#FF4444", 0.8, 2, 8

if __name__ == "__main__":
    app.run(debug=True)