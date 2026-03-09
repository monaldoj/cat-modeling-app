import os
from databricks import sql
import databricks
from databricks.sdk.core import Config
import pandas as pd
import numpy as np
from plotly.graph_objs.layout import updatemenu
from shapely.geometry import Polygon
import dash
from dash import dcc, html, Input, Output, State, callback_context, no_update
import plotly.express as px
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
from databricks.sdk.core import Config
import dash_leaflet as dl
from dash.dependencies import Input, Output, ALL
from dash_extensions.javascript import arrow_function
from dash_extensions.enrich import DashProxy, Input, Output, html
import flask
import json
import datetime as dt
from shapely.geometry import Polygon, Point, box
from shapely import wkt
import threading
import requests
from urllib.parse import quote
from assets.svg_icons import svg_pin_icon

# Set up the app
app = DashProxy(__name__)

# Check for environment variables but don't fail if they're not set (for development)
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
DASHBOARD_URL = os.getenv("DASHBOARD_URL")
default_catalog = "financial_services"
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
# List to hold streamed responses
response_list = []
stream_complete = True
# List to hold draft message responses
draft_message_response_list = []
draft_message_stream_complete = True

global_databricks_sp_token = None

icon_style_core = dict(iconUrl=svg_pin_icon("#4CAF50"), iconSize=[40, 40], iconAnchor=[20, 40])
icon_style_secondary = dict(iconUrl=svg_pin_icon("#FFFF33"), iconSize=[40, 40], iconAnchor=[20, 40])
icon_style_clicked = dict(iconUrl=svg_pin_icon("#FF0000"), iconSize=[60, 60], iconAnchor=[30, 60])
card_style_core = {
    "backgroundColor": "#2C2C2C",
    "padding": "12px",
    "marginBottom": "10px",
    "borderRadius": "6px",
    "border": "2px solid #4CAF50",
    "fontFamily": "Helvetica",
    "fontSize": "12px",
    "cursor": "pointer",
    "transition": "all 0.3s ease",
    "width": "100%",
    "textAlign": "left"
}
card_style_secondary = {
    **card_style_core,
    "backgroundColor": "#2C2C2C",
    "border": "2px solid #FFFF33",
    "boxShadow": "0 4px 8px rgba(255, 255, 51, 0.3)",
}
card_style_clicked = {
    **card_style_core,
    "backgroundColor": "#2C2C2C",
    "border": "2px solid #FF0000",
    "boxShadow": "0 4px 8px rgba(76, 175, 80, 0.3)",
    "transform": "translateX(5px)"
}

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

def get_databricks_sp_token():
    global global_databricks_sp_token
    if global_databricks_sp_token:
        return global_databricks_sp_token
    else:
        DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

        if not DATABRICKS_TOKEN:
            print("DATABRICKS_TOKEN not set in environment variables, using SP authentication.")
            host = get_databricks_server_hostname()
            DATABRICKS_HOST = "https://" + host
            CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID")
            CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET")

            if not CLIENT_ID or not CLIENT_SECRET:
                print("DATABRICKS_CLIENT_ID or DATABRICKS_CLIENT_SECRET not set in environment variables, using on-behalf-of authentication via get_databricks_token().")
                DATABRICKS_TOKEN = get_databricks_token()
                global_databricks_sp_token = DATABRICKS_TOKEN
                return DATABRICKS_TOKEN

            # OAuth endpoint for Databricks on GCP
            TOKEN_URL = f"{DATABRICKS_HOST}/oidc/v1/token"
            token_response = requests.post(
                TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "scope": "all-apis",
                },
                auth=(CLIENT_ID, CLIENT_SECRET),
            )

            token_response.raise_for_status()
            DATABRICKS_TOKEN = token_response.json()["access_token"]

            print("SP access token retrieved successfully")
        else:
            print("TOKEN SET AS DATABRICKS_TOKEN, USING ENV TOKEN.")

    global_databricks_sp_token = DATABRICKS_TOKEN

    return DATABRICKS_TOKEN


def sqlQuery(query: str) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    # print("RUNNING QUERY:", query)
    DATABRICKS_SERVER_HOSTNAME = get_databricks_server_hostname()
    DATABRICKS_TOKEN = get_databricks_sp_token()
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

def fmapi_stream_ai_assessment(token, host, model, clicked_property_data, event_name):
    global response_list
    global stream_complete

    print("SETTING STREAM COMPLETE TO FALSE")
    response_list = []
    stream_complete = False

    if clicked_property_data and event_name:
        host = host.replace("https://", "")
        databricks_host = "https://" + host # get_databricks_server_hostname()
        databricks_token = token # get_databricks_token()
        # model = os.getenv("LLM_ENDPOINT") # 'databricks-meta-llama-3-3-70b-instruct'
        endpoint = f"{databricks_host}/serving-endpoints/{model}/invocations"
        print("ENDPOINT:", endpoint)

        # Define the headers, including the authorization token
        headers = {
            "Authorization": f"Bearer {databricks_token}",
            "Content-Type": "application/json"
        }

        # Define the payload for the request
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "You are an insurance risk assessor. You are given a property and you need to write a brief payout assessment of the property with respect to a catasrophic event."},
                {"role": "user", "content": f"Please write a one-sentence payout assessment of the potential damage to the following property as inflicted by the catastrophic event: {event_name} \n\n {clicked_property_data}"}
            ],
            # "max_tokens": 256,
            "stream": True  # Enable streaming
        }

        # print(endpoint, payload)
        # Make the POST request with streaming enabled
        response = requests.post(endpoint, headers=headers, data=json.dumps(payload), stream=True)
        # print(response)
        # print("RESPONSE STATUS CODE:", response.status_code, type(response.status_code))
        # print("RESPONSE:", response.text)
        # if response.status_code == 403:
        #     databricks_sp_token = get_databricks_sp_token()
        #     headers["Authorization"] = f"Bearer {databricks_sp_token}"
        #     response = requests.post(endpoint, headers=headers, data=json.dumps(payload), stream=True)
        #     print("SP RESPONSE STATUS CODE:", response.status_code, type(response.status_code))
        #     print("SP RESPONSE:", response.text)

        try:
            for chunk in response.iter_content(chunk_size=None):
                if chunk:
                    # print("CHUNK:", chunk)
                    new_chunks = chunk.decode('utf-8').replace('\n'," ").split('data:')
                    new_chunks = [x.strip() for x in new_chunks if len(x.strip())>0]
                    # print("NEW_CHUNKS", new_chunks)
                    for new_chunk in new_chunks:
                        # if new_chunk == "[DONE]":
                        #     break
                        # print("NEW_CHUNK", new_chunk)
                        new_chunk_data = json.loads(new_chunk)
                        # print("NEW_CHUNK_DATA:", new_chunk_data)
                        if new_chunk_data['choices'][0]['finish_reason'] == 'stop':
                            break
                        response_list.append(new_chunk_data['choices'][0]['delta']['content'])
            response_list.append(None)
        except Exception as e:
            response_list.append(None)
            print(f"An error occurred: {e}")

    print("SETTING STREAM COMPLETE TO TRUE")
    stream_complete = True

def fmapi_stream_draft_message(token, host, model, clicked_property_data, event_name):
    global draft_message_response_list
    global draft_message_stream_complete

    print("SETTING DRAFT MESSAGE STREAM COMPLETE TO FALSE")
    draft_message_response_list = []
    draft_message_stream_complete = False

    if clicked_property_data and event_name:
        host = host.replace("https://", "")
        databricks_host = "https://" + host # get_databricks_server_hostname()
        databricks_token = token # get_databricks_token()
        # model = 'databricks-meta-llama-3-3-70b-instruct'
        # model = 'databricks-dbrx-instruct'
        endpoint = f"{databricks_host}/serving-endpoints/{model}/invocations"
        print("ENDPOINT:", endpoint)

        # Define the headers, including the authorization token
        headers = {
            "Authorization": f"Bearer {databricks_token}",
            "Content-Type": "application/json"
        }

        # Define the payload for the request - DIFFERENT from AI Assessment
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "You are a professional insurance claims adjuster. You are drafting a message to the property owner about their claim."},
                {"role": "user", "content": f"Please draft a brief, professional message to the property owner regarding the catastrophic event: {event_name} and their property. The message should be empathetic and informative. \n\n Property details: {clicked_property_data}"}
            ],
            # "max_tokens": 256,
            "stream": True  # Enable streaming
        }

        # print(endpoint, payload)
        # Make the POST request with streaming enabled
        response = requests.post(endpoint, headers=headers, data=json.dumps(payload), stream=True)
        print(response)
        try:
            for chunk in response.iter_content(chunk_size=None):
                if chunk:
                    # print("CHUNK:", chunk)
                    new_chunks = chunk.decode('utf-8').replace('\n'," ").split('data:')
                    new_chunks = [x.strip() for x in new_chunks if len(x.strip())>0]
                    # print("NEW_CHUNKS", new_chunks)
                    for new_chunk in new_chunks:
                        # if new_chunk == "[DONE]":
                        #     break
                        # print("NEW_CHUNK", new_chunk)
                        new_chunk_data = json.loads(new_chunk)
                        # print("NEW_CHUNK_DATA:", new_chunk_data)
                        if new_chunk_data['choices'][0]['finish_reason'] == 'stop':
                            break
                        draft_message_response_list.append(new_chunk_data['choices'][0]['delta']['content'])
            draft_message_response_list.append(None)
        except Exception as e:
            draft_message_response_list.append(None)
            print(f"An error occurred: {e}")

    print("SETTING DRAFT MESSAGE STREAM COMPLETE TO TRUE")
    draft_message_stream_complete = True

# Fetch the all h3 data
def get_data(catastrophe_type=None, resolution=9, bounds=None, column_resolution=None):
    stime = dt.datetime.now()
    
    catalog = "financial_services"
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
                        MAX({catastrophe_type}) as value
                        -- AVG(coalesce({catastrophe_type}, 0)) as value
                    FROM {default_catalog}.{default_schema}.cat_risk_scores_h3 -- {catalog}.{schema}.{table}
                    WHERE {f"h3_toparent({column}, {resolution}) IN (SELECT EXPLODE(H3_COVERASH3('{bounds_wkt}', {resolution})))" if bounds_wkt else "1=1"}
                    GROUP BY h3_cell_id
                    HAVING value > 0
                    )
                    SELECT h3_cell_id, h3_boundaryasgeojson(h3_cell_id) as hex_boundary,
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
    DATABRICKS_TOKEN = get_databricks_sp_token()
    with sql.connect(
        http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        access_token=DATABRICKS_TOKEN
    ) as connection:
        with connection.cursor() as cursor:
            query = f"""
                SELECT DISTINCT event_name
                FROM {default_catalog}.{default_schema}.cat_events
                ORDER BY event_name
            """
            cursor.execute(query)
            events = cursor.fetchall()
            print("EVENTS:", events)
            events = [event.event_name for event in events]
            # print(catalogs)
        return events

def get_event_details(event_name):
    DATABRICKS_SERVER_HOSTNAME = get_databricks_server_hostname()
    DATABRICKS_TOKEN = get_databricks_sp_token()
    with sql.connect(
        http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        access_token=DATABRICKS_TOKEN
    ) as connection:
        with connection.cursor() as cursor:
            query = f"""
                SELECT *
                FROM {default_catalog}.{default_schema}.cat_events
                WHERE event_name = '{event_name}'
            """
            cursor.execute(query)
            events = cursor.fetchall()
            event_details = [event.wkt for event in events][0]
            # print(catalogs)
        return event_details

def get_affected_properties(event_name=None):
    DATABRICKS_SERVER_HOSTNAME = get_databricks_server_hostname()
    DATABRICKS_TOKEN = get_databricks_sp_token()
    with sql.connect(
        http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        access_token=DATABRICKS_TOKEN
    ) as connection:
        with connection.cursor() as cursor:
            event_wkt = get_event_details(event_name)
            buffer_wkt = create_buffer_polygon(event_wkt, 20, units="miles")
            query = f"""
                SELECT portfolio_id, id as property_id, property_value, name, lat, lon, housenumber, street, city, state, postcode,
                       CASE WHEN ST_CONTAINS(ST_GEOMFROMTEXT('{event_wkt}'), ST_POINT(lon, lat)) THEN property_value * 0.75 ELSE property_value * 0.25 END as property_payout,
                       ST_CONTAINS(ST_GEOMFROMTEXT('{event_wkt}'), ST_POINT(lon, lat)) as core_polygon,
                       ST_CONTAINS(ST_GEOMFROMTEXT('{buffer_wkt}'), ST_POINT(lon, lat)) as secondary_polygon
                FROM {default_catalog}.{default_schema}.portfolio
                WHERE ST_CONTAINS(ST_GEOMFROMTEXT('{buffer_wkt}'), ST_POINT(lon, lat))
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
        return 3
    elif zoom == 5:
        return 3
    elif zoom == 6:
        return 4
    elif zoom == 7:
        return 4
    elif zoom == 8:
        return 5
    elif zoom == 9:
        return 6
    elif zoom == 10:
        return 6
    elif zoom == 11:
        return 7
    elif zoom == 12:
        return 8
    elif zoom == 13:
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

def create_buffer_polygon(wkt_string, buffer_distance, units="degrees"):
    """
    Create a buffer polygon around the input polygon (in WKT format) at the specified distance.
    Args:
        wkt_string (str): WKT string describing the input polygon.
        buffer_distance (float): Buffer distance.
        units (str): Units for the buffer distance ("degrees" or "miles").
    Returns:
        str: WKT string of the buffered polygon, or None on error.
    """
    try:
        poly = wkt.loads(wkt_string)
        distance = buffer_distance
        if units == "miles":
            # Approximate conversion: 1 degree (lat/lon) ~= 69 miles
            distance = buffer_distance / 69.0
        # else, assume degrees
        buffered_poly = poly.buffer(distance)
        return buffered_poly.wkt
    except Exception as e:
        print(f"Error creating buffer: {e}")
        return None

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

def data_to_polygons(map_data, catastrophe_type=None):
    # print("map_data:", map_data)
    hex_centers_lats = []
    hex_centers_lngs = []
    hex_boundaries_polygons = []
    hex_boundaries_geojson = {
        "type": "FeatureCollection",
        "features": []
    }
    dlPolygons = []

    stime = dt.datetime.now()
    print(f"DATA TO POLYGONS: {len(map_data)} rows, TIME: {stime}")
    for i in range(len(map_data)):
        hex_cell_id = map_data.iloc[i]['h3_cell_id']
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

        # Create a deterministic ID that includes catastrophe type
        # This ensures polygons are recreated when colors change due to different data
        # while maintaining performance by reusing polygons with same ID when data is identical
        hex_id = f"{hex_cell_id}-{catastrophe_type}" if catastrophe_type else str(hex_cell_id)

        dlPolygons.append(dl.Polygon(
            id={"type": "hex-polygon", "index": hex_id},
            positions=hex_boundaries_polygon,
            fillColor=style['fillColor'],
            color=style['color'],
            weight=style['weight'],
            opacity=style['opacity'],
            fillOpacity=style['fillOpacity']
            )
        )
    # print([x.fillColor for x in dlPolygons][0:7])
    # print(f"DATA TO POLYGONS TOOK: {dt.datetime.now() - stime}")
    return dlPolygons

    # polygons = []
    # for i in range(len(map_data)):
    #     polygons.append(dl.Polygon(positions=map_data.iloc[i]['hex_boundary'], color="#39FF14", fillOpacity=0.04))
    # return polygons

app.layout = html.Div(
    [
        dcc.Location(id="url"),
        dcc.Store(id='property-data-store', data=[]),
        dcc.Store(id='highlighted-property', data=None),
        dcc.Store(id='clicked-property', data=None),
        dcc.Store(id='active-ai-assessment', data=None),
        dcc.Store(id='active-draft-message', data=None),
        dcc.Store(id='selected-portfolio-id', data=None),
        dcc.Store(id='overlay-visible', data=False),
        dcc.Store(id='nav-collapsed', data=True),
        dcc.Store(id='active-page', data='map'),
        # Navigation Menu
        html.Div(
            id='nav-menu',
            children=[
                html.Div(
                    [
                        html.Div(
                            "☰",
                            id="nav-toggle",
                            style={
                                "fontSize": "24px",
                                "cursor": "pointer",
                                "padding": "10px",
                                "color": "#FFFFFF",
                                "textAlign": "left",
                                "borderBottom": "1px solid #4A4A4A"
                            }
                        ),
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Img(src="/assets/pinOutlinedIcon-light.svg", style={"marginRight": "10px", "width": "16px", "height": "16px"}),
                                        html.Span("Interactive Map", id="map-nav-text")
                                    ],
                                    id="map-nav-item",
                                    n_clicks=0,
                                    style={
                                        "padding": "15px",
                                        "cursor": "pointer",
                                        "color": "#FFFFFF",
                                        "backgroundColor": "#4A4A4A",
                                        "borderBottom": "1px solid #3A3A3A",
                                        "display": "flex",
                                        "alignItems": "center"
                                    }
                                ),
                                html.Div(
                                    [
                                        html.Img(src="/assets/dashboardIcon-light.svg", style={"marginRight": "10px", "width": "16px", "height": "16px"}),
                                        html.Span("Dashboard", id="dashboard-nav-text")
                                    ],
                                    id="dashboard-nav-item",
                                    n_clicks=0,
                                    style={
                                        "padding": "15px",
                                        "cursor": "pointer",
                                        "color": "#FFFFFF",
                                        "backgroundColor": "#3A3A3A",
                                        "borderBottom": "1px solid #3A3A3A",
                                        "display": "flex",
                                        "alignItems": "center"
                                    }
                                )
                            ]
                        )
                    ]
                )
            ],
            style={
                "position": "fixed",
                "left": "0",
                "top": "0",
                "height": "100vh",
                "width": "250px",
                "backgroundColor": "#2C2C2C",
                "zIndex": "10001",
                "transition": "transform 0.3s ease",
                "boxShadow": "2px 0 5px rgba(0,0,0,0.3)",
                "fontFamily": "Helvetica"
            }
        ),
        # Map content container
        html.Div(
            children=[
                # Title
                html.Div(
                    "Peril Predicts: Parametric Payouts",
                    style={
                        "fontSize": "28px",
                        "fontWeight": "bold",
                        "color": "#FFFFFF",
                        "fontFamily": "Helvetica",
                        "textAlign": "left",
                        "padding": "20px",
                        "paddingLeft": "30px",
                        "backgroundColor": "#2C2C2C",
                        "marginBottom": "0px"
                    }
                ),
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
                    ],
            style={
                "backgroundColor": "#3A3A3A",
                "padding": "15px",
                "borderRadius": "8px",
                # "marginBottom": "20px",
                "boxShadow": "0 2px 4px rgba(0,0,0,0.3)"
            }
                ),
                # Two column layout for map and property list
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
                                        dl.LayerGroup(id="map-hex-polygons", children=[]),
                                        dl.LayerGroup(id="map-event-polygons", children=[]),
                                        dl.LayerGroup(id="map-property-markers", children=[]) #dl.Polygon(color='#39FF14', positions=[[25.2, -81.6], [25.2, -79.8], [26.6, -79.8], [26.6, -81.6]])])
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
                                html.Div(id="legend-container", children=legend),
                                # Hidden div to trigger marker highlighting
                                html.Div(id="marker-highlight-trigger", style={"display": "none"})
                            ],
                            id='loading-map',
                            type='default',
                            color='#3A3A3A',
                            overlay_style={"visibility":"visible", "filter": "blur(3px)"},
                        ),
                    dcc.Interval(id="interval-component", interval=200, n_intervals=0, disabled=True),  # Check every n milliseconds
                    dcc.Interval(id="interval-component-draft", interval=200, n_intervals=0, disabled=True),  # Check every n milliseconds for draft messages
                    dcc.Interval(id="dashboard-interval-ai", interval=200, n_intervals=0, disabled=True),  # Dashboard AI Assessment interval
                    dcc.Interval(id="dashboard-interval-email", interval=200, n_intervals=0, disabled=True),  # Dashboard Draft Email interval
                    ],
                    style={"width": "75%", "display": "inline-block", "verticalAlign": "top", "position": "relative"}
                ),
                # Right column - Property List (25%)
                html.Div(
                    [
                        html.Div(
                            [
                                html.H4("Affected Properties", 
                                       style={
                                           "color": "#FFFFFF", 
                                           "fontFamily": "Helvetica", 
                                           "fontWeight": "bold",
                                           "marginBottom": "10px",
                                           "textAlign": "center"
                                       }),
                                dcc.Dropdown(
                                    id="portfolio-dropdown",
                                    placeholder="All Portfolios",
                                    value='all',
                                    style={
                                        "width": "100%",
                                        "backgroundColor": "#FFFFFF",
                                        "fontFamily": "Helvetica",
                                        "fontSize": "12px",
                                        "marginBottom": "15px"
                                    },
                                    clearable=True
                                ),
                                html.Div(id="property-agg",
                                        style={
                                            "color": "#CCCCCC",
                                            "fontFamily": "Helvetica",
                                            "fontSize": "12px",
                                            "marginBottom": "15px",
                                            "textAlign": "center"
                                        }),
                                dcc.Loading(
                                    id="loading-properties",
                                    type="circle",
                                    children=html.Div(
                                        id="property-list",
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
                # Portfolio Analysis Overlay
                html.Div(
                    id="portfolio-analysis-overlay",
                    children=[
                html.Div(
                    [
                        # Header with close button
                        html.Div(
                            [
                                html.H3("Property Analysis", 
                                       style={
                                           "color": "#FFFFFF",
                                           "fontFamily": "Helvetica",
                                           "fontWeight": "bold",
                                           "margin": "0",
                                           "flex": "1"
                                       }),
                                dbc.Button(
                                    "✕ Close property analysis",
                                    id="close-portfolio-analysis-btn",
                                    style={
                                        "backgroundColor": "#FF4444",
                                        "color": "#FFFFFF",
                                        "cursor": "pointer",
                                        "borderRadius": "4px",
                                        "fontSize": "14px",
                                        "fontFamily": "Helvetica",
                                        "fontWeight": "bold",
                                        "border": "1px solid #FF4444",
                                        "padding": "8px 16px"
                                    }
                                )
                            ],
                            style={
                                "display": "flex",
                                "justifyContent": "space-between",
                                "alignItems": "center",
                                "marginBottom": "20px",
                                "paddingBottom": "15px",
                                "borderBottom": "2px solid #4A4A4A"
                            }
                        ),
                        # Content area with dashboard and chat
                        html.Div(
                            [
                                # Scrollable container for all content
                                html.Div(
                                    [
                                        # Top row: Images (left) and AI/Email (right)
                                        html.Div(
                                            [
                                                # Left side - Property Images (50%)
                                                html.Div(
                                                    [
                                                        html.H5("Property Images",
                                                               style={
                                                                   "color": "#FFFFFF",
                                                                   "fontFamily": "Helvetica",
                                                                   "fontSize": "14px",
                                                                   "marginBottom": "15px",
                                                                   "fontWeight": "bold"
                                                               }),
                                                        html.Div(
                                                            [
                                                                # Left column - Original image
                                                                html.Div(
                                                                    [
                                                                        html.H6("Original",
                                                                               style={
                                                                                   "color": "#CCCCCC",
                                                                                   "fontFamily": "Helvetica",
                                                                                   "fontSize": "12px",
                                                                                   "marginBottom": "8px",
                                                                                   "fontWeight": "bold"
                                                                               }),
                                                                        html.Div(
                                                                            id="property-image-original-container",
                                                                            children=[],
                                                                            style={
                                                                                "minHeight": "200px",
                                                                                "display": "flex",
                                                                                "alignItems": "center",
                                                                                "justifyContent": "center"
                                                                            }
                                                                        )
                                                                    ],
                                                                    style={
                                                                        "width": "48%",
                                                                        "display": "inline-block",
                                                                        "verticalAlign": "top"
                                                                    }
                                                                ),
                                                                # Right column - Damaged image
                                                                html.Div(
                                                                    [
                                                                        html.H6("Damaged",
                                                                               style={
                                                                                   "color": "#CCCCCC",
                                                                                   "fontFamily": "Helvetica",
                                                                                   "fontSize": "12px",
                                                                                   "marginBottom": "8px",
                                                                                   "fontWeight": "bold"
                                                                               }),
                                                                        html.Div(
                                                                            id="property-image-damaged-container",
                                                                            children=[],
                                                                            style={
                                                                                "minHeight": "200px",
                                                                                "display": "flex",
                                                                                "alignItems": "center",
                                                                                "justifyContent": "center"
                                                                            }
                                                                        )
                                                                    ],
                                                                    style={
                                                                        "width": "48%",
                                                                        "display": "inline-block",
                                                                        "verticalAlign": "top",
                                                                        "marginLeft": "4%"
                                                                    }
                                                                )
                                                            ],
                                                            style={
                                                                "display": "flex",
                                                                "justifyContent": "space-between",
                                                                "width": "100%"
                                                            }
                                                        )
                                                    ],
                                                    style={
                                                        "width": "48%",
                                                        "padding": "15px",
                                                        "backgroundColor": "#2C2C2C",
                                                        "borderRadius": "6px",
                                                        "border": "1px solid #4A4A4A"
                                                    }
                                                ),
                                                # Right side - AI Assessment and Draft Email stacked (50%)
                                                html.Div(
                                                    [
                                                        # AI Assessment box
                                                        html.Div(
                                                            [
                                                                html.Button(
                                                                    "🤖 AI Assessment",
                                                                    id="dashboard-ai-assessment-btn",
                                                                    n_clicks=0,
                                                                    style={
                                                                        "backgroundColor": "#4A4A4A",
                                                                        "color": "#FFFFFF",
                                                                        "padding": "8px 16px",
                                                                        "border": "1px solid #5A5A5A",
                                                                        "borderRadius": "4px",
                                                                        "fontFamily": "Helvetica",
                                                                        "fontSize": "12px",
                                                                        "fontWeight": "bold",
                                                                        "cursor": "pointer",
                                                                        "width": "100%",
                                                                        "transition": "background-color 0.2s ease",
                                                                        "marginBottom": "10px"
                                                                    }
                                                                ),
                                                                html.Div(
                                                                    id="dashboard-ai-assessment-text",
                                                                    style={
                                                                        "display": "none",
                                                                        "padding": "10px",
                                                                        "backgroundColor": "#1A1A1A",
                                                                        "borderRadius": "4px",
                                                                        "border": "1px solid #5A5A5A",
                                                                        "color": "#FFFFFF",
                                                                        "fontFamily": "Helvetica",
                                                                        "fontSize": "12px",
                                                                        "lineHeight": "1.5",
                                                                        "whiteSpace": "pre-wrap"
                                                                    }
                                                                )
                                                            ],
                                                            style={
                                                                "marginBottom": "15px",
                                                                "padding": "15px",
                                                                "backgroundColor": "#2C2C2C",
                                                                "borderRadius": "6px",
                                                                "border": "1px solid #4A4A4A"
                                                            }
                                                        ),
                                                        # Draft Email box
                                                        html.Div(
                                                            [
                                                                html.Button(
                                                                    "✉️ Draft Email",
                                                                    id="dashboard-draft-email-btn",
                                                                    n_clicks=0,
                                                                    style={
                                                                        "backgroundColor": "#4A4A4A",
                                                                        "color": "#FFFFFF",
                                                                        "padding": "8px 16px",
                                                                        "border": "1px solid #5A5A5A",
                                                                        "borderRadius": "4px",
                                                                        "fontFamily": "Helvetica",
                                                                        "fontSize": "12px",
                                                                        "fontWeight": "bold",
                                                                        "cursor": "pointer",
                                                                        "width": "100%",
                                                                        "transition": "background-color 0.2s ease",
                                                                        "marginBottom": "10px"
                                                                    }
                                                                ),
                                                                html.Div(
                                                                    id="dashboard-draft-email-text",
                                                                    style={
                                                                        "display": "none",
                                                                        "padding": "10px",
                                                                        "backgroundColor": "#1A1A1A",
                                                                        "borderRadius": "4px",
                                                                        "border": "1px solid #5A5A5A",
                                                                        "color": "#FFFFFF",
                                                                        "fontFamily": "Helvetica",
                                                                        "fontSize": "12px",
                                                                        "lineHeight": "1.5",
                                                                        "whiteSpace": "pre-wrap"
                                                                    }
                                                                )
                                                            ],
                                                            style={
                                                                "padding": "15px",
                                                                "backgroundColor": "#2C2C2C",
                                                                "borderRadius": "6px",
                                                                "border": "1px solid #4A4A4A"
                                                            }
                                                        )
                                                    ],
                                                    style={
                                                        "width": "48%",
                                                        "display": "flex",
                                                        "flexDirection": "column"
                                                    }
                                                )
                                            ],
                                            style={
                                                "display": "flex",
                                                "justifyContent": "space-between",
                                                "width": "100%",
                                                "marginBottom": "20px"
                                            }
                                        ),
                                        # Bottom row: Dashboard iframe (full width)
                                        html.Div(
                                            [
                                                html.Iframe(
                                                    id="dashboard-iframe-overlay",
                                                    src="",  # Will be set dynamically via callback
                                                    style={
                                                        "width": "100%",
                                                        "height": "600px",
                                                        "border": "1px solid #4A4A4A",
                                                        "borderRadius": "4px"
                                                    }
                                                )
                                            ],
                                            style={
                                                "width": "100%"
                                            }
                                        )
                                    ],
                                    style={
                                        "maxHeight": "calc(80vh - 20px)",
                                        "overflowY": "auto",
                                        "overflowX": "hidden",
                                        "paddingRight": "10px",
                                        "width": "100%"
                                    }
                                )
                            ],
                            style={
                                "display": "flex",
                                "width": "100%"
                            }
                        )
                    ],
                    style={
                        "position": "relative",
                        "width": "90%",
                        "maxWidth": "1600px",
                        "height": "85vh",
                        "backgroundColor": "#2C2C2C",
                        "borderRadius": "8px",
                        "padding": "30px",
                        "boxShadow": "0 4px 20px rgba(0,0,0,0.5)",
                        "zIndex": "10001"
                    }
                )
            ],
            style={
                "display": "none",  # Hidden by default
                "position": "fixed",
                "top": "0",
                "left": "0",
                "width": "100%",
                "height": "100%",
                "backgroundColor": "rgba(0, 0, 0, 0.8)",
                "zIndex": "10000",
                "justifyContent": "center",
                "alignItems": "center"
                }
                )
            ],  # End of map content children
            id='map-content',
            style={
                "marginLeft": "250px",
                "transition": "margin-left 0.3s ease",
                "width": "calc(100% - 250px)"
            }
        ),
        # Dashboard content container
        html.Div(
            id='dashboard-content',
            children=[
                # Title
                html.Div(
                    "Peril Predicts: Parametric Payouts",
                    style={
                        "fontSize": "28px",
                        "fontWeight": "bold",
                        "color": "#FFFFFF",
                        "fontFamily": "Helvetica",
                        "textAlign": "left",
                        "padding": "20px",
                        "paddingLeft": "30px",
                        "backgroundColor": "#2C2C2C"
                    }
                ),
                html.Iframe(
                    id='dashboard-iframe',
                    src=DASHBOARD_URL if DASHBOARD_URL else "",
                    style={
                        "width": "100%",
                        "height": "calc(100vh - 68px)",
                        "border": "none"
                    }
                )
            ],
            style={
                "display": "none",
                "marginLeft": "250px",
                "transition": "margin-left 0.3s ease",
                "width": "calc(100% - 250px)"
            }
        )
    ],
    style={"backgroundColor": "#29323C"},
    
)

# Add callback for refresh button and initial load
@app.callback(
    [Output('map-hex-polygons', 'children', allow_duplicate=True),
     Output('refresh-button', 'style')],
    Input('refresh-button', 'n_clicks'),
    [State("map", "center"),
     State("map", "zoom"),
     State("map", "bounds"),
     State("catastrophe-dropdown", "value"),
    #  State("event-dropdown", "value"),
    #  State('map-hex-polygons', 'children'),
     ],
     prevent_initial_call=True
)
def update_map(n_clicks, center, zoom, bounds, catastrophe_type): #, event_name, children):
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

        print(f"Global Zoom: {global_zoom}")
        resolution = zoom_to_h3_resolution(global_zoom)

        # Fetch new data
        new_map_data = get_data(catastrophe_type=catastrophe_type, bounds=global_bounds, resolution=resolution, column_resolution=resolution)
        
        print("new_map_data length:", len(new_map_data))
        new_polygons = data_to_polygons(new_map_data, catastrophe_type=catastrophe_type)
        print("new_polygons length:", len(new_polygons))

        # print([x.fillColor for x in new_polygons][0:7])
        # Keep event polygon and property markers in children
        # stime = dt.datetime.now()
        # print(f"CHILDREN: {len(children)}, TIME: {dt.datetime.now()-stime}")
        # print(f"NEW POLYGONS: {len(new_polygons)}, TIME: {dt.datetime.now()-stime}")
        # filtered_children = [x for x in children 
        #                    if 'id' in x.get('props', {}) 
        #                    and (isinstance(x['props']['id'], dict) 
        #                         and x['props']['id'].get('type') in ['event-polygon', 'property-marker'])]
        # # print(f"FILTERED CHILDREN: {len(filtered_children)}, TIME: {dt.datetime.now()-stime}")
        # polygons = new_polygons + filtered_children
        # print(f"POLYGONS: {len(polygons)}, TIME: {dt.datetime.now()-stime}")
        # print([x.fillColor for x in polygons][0:7])
        print("Map refreshed successfully!")
        return new_polygons, active_style


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
    # [Input('event-dropdown', 'id'),
    [Input("url", "pathname")],
    prevent_initial_call=False
)
def populate_events(pathname):
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

# Callback to populate portfolio dropdown when properties are loaded
@app.callback(
    Output('portfolio-dropdown', 'options'),
    Input('property-data-store', 'data'),
    prevent_initial_call=True
)
def populate_portfolio_dropdown(property_data):
    """Populate the portfolio dropdown with unique portfolio IDs from loaded properties"""
    if not property_data:
        return []
    
    # Extract unique portfolio IDs
    portfolio_ids = sorted(list(set([prop['portfolio_id'] for prop in property_data])))
    
    # Create dropdown options with "All Portfolios" as the first option
    options = [{'label': 'All Portfolios', 'value': 'all'}]
    options.extend([{'label': f'Portfolio {pid}', 'value': pid} for pid in portfolio_ids])
    
    return options

# Callback to update both property list and markers when event is selected
@app.callback(
    [Output('map', 'viewport'),
     Output('property-list', 'children'),
     Output('property-agg', 'children'),
     Output('property-data-store', 'data'),
     Output('map-event-polygons', 'children'),
     Output('map-property-markers', 'children'),
     Output('map-hex-polygons', 'children'),
     Output('portfolio-dropdown', 'value')],
    [Input('event-dropdown', 'value')],
    [State('map-hex-polygons', 'children')],
    prevent_initial_call=True
)
def update_property_list_and_markers(event_name, hex_children):
    """Update the property list and map markers when an event is selected"""
    
    stime = dt.datetime.now()

    global global_bounds
    viewport = dict(bounds=global_bounds)
    print(f"intial event_name: {event_name}")

    if event_name is None:
        return viewport, html.Div(
            "Select an event to view affected properties",
            style={
                "color": "#CCCCCC",
                "fontFamily": "Helvetica",
                "fontSize": "14px",
                "textAlign": "center",
                "padding": "20px"
            }
        ), "", [], [], [], hex_children, 'all'
    
    try:
        event_wkt = get_event_details(event_name)
        buffer_wkt = create_buffer_polygon(event_wkt, 20, units="miles")
        global_bounds = wkt_to_bounds(buffer_wkt)
        viewport = dict(
            bounds=global_bounds,
            transition="flyTo",
            duration=3000
        )
        event_polygon = [list([coord[1], coord[0]]) for coord in wkt_to_geojson(event_wkt)['coordinates'][0]][:-1]
        buffer_polygon = [list([coord[1], coord[0]]) for coord in wkt_to_geojson(buffer_wkt)['coordinates'][0]][:-1]
        event_polygon_element = dl.Polygon(id={"type": "event-polygon", "index": "event-polygon-core"}, positions=event_polygon, color="#39FF14", fillOpacity=0.1)
        buffer_polygon_element = dl.Polygon(id={"type": "event-polygon", "index": "buffer-polygon-secondary"}, positions=buffer_polygon, color="#FFFF33", fillOpacity=0.05)
        
        # Filter updated_children to only include elements where x['props']['positions'] coordinates overlap with the viewport coordinates using shapely
        viewport_filter = wkt.loads(buffer_wkt)
        updated_hex_children = []
        for child in hex_children:
            child_polygon = Polygon([(x[1], x[0]) for x in child['props']['positions']])
            if child_polygon.is_valid and child_polygon.intersects(viewport_filter):
                updated_hex_children.append(child)

        print(f"Time taken to filter children: {dt.datetime.now() - stime}")
        
        # updated_children = [x for x in children if x['props']['id']['type'] != 'event-polygon']
        event_polygons_children = []
        event_polygons_children.append(event_polygon_element)
        event_polygons_children.append(buffer_polygon_element)
        print(f"Time taken to create event polygon: {dt.datetime.now() - stime}")

        # Fetch properties affected by the event
        properties_df = get_affected_properties(event_name)
        print(f"Time taken to fetch properties: {dt.datetime.now() - stime}")
        
        if properties_df.empty:
            return viewport, html.Div(
                "No properties found in this event area",
                style={
                    "color": "#CCCCCC",
                    "fontFamily": "Helvetica",
                    "fontSize": "14px",
                    "textAlign": "center",
                    "padding": "20px"
                }
            ), html.Div("0 properties"), [], event_polygons_children, [], updated_hex_children, 'all'
        
        # Store property data for markers
        property_data = properties_df.to_dict('records')
        
        # Create property cards with hover interaction
        property_cards = []
        for idx, row in properties_df.iterrows():
            card = html.Div(
                [
                    html.Button(
                        [
                            html.Div(
                                [
                                    html.Div(
                                        [
                                            html.Strong("Portfolio ID: ", style={"color": "#FFFFFF"}),
                                            html.Span(str(row['portfolio_id']), style={"color": "#CCCCCC"})
                                        ],
                                        style={"marginBottom": "5px"}
                                    ),
                                    html.Div(
                                        [
                                            html.Strong("Property ID: ", style={"color": "#FFFFFF"}),
                                            html.Span(str(row['property_id']), style={"color": "#CCCCCC"})
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
                                        style={"marginBottom": "10px"}
                                    ),
                                    html.Button(
                                        "📊 Property Analysis",
                                        id={"type": "property-analysis-btn", "index": str(row['property_id'])},
                                        n_clicks=0,
                                        style={
                                            "backgroundColor": "#4CAF50",
                                            "color": "#FFFFFF",
                                            "padding": "8px 16px",
                                            "border": "1px solid #4CAF50",
                                            "borderRadius": "4px",
                                            "fontFamily": "Helvetica",
                                            "fontSize": "11px",
                                            "fontWeight": "bold",
                                            "cursor": "pointer",
                                            "width": "100%",
                                            "textAlign": "center",
                                            "transition": "background-color 0.2s ease",
                                        }
                                    )
                                ]
                            )
                        ],
                        id={"type": "property-card", "index": str(row['property_id'])},
                        n_clicks=0,
                        className="property-card",
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
                ],
                style={"marginBottom": "10px"}
            )
            property_cards.append(card)

        print(f"Time taken to create property cards: {dt.datetime.now() - stime}")
        total_count = len(properties_df)
        total_value = properties_df['property_value'].sum()
        total_payout = properties_df['property_payout'].sum()
        
        # Create formatted aggregate statistics with bolded numbers
        agg_div = html.Div([
            html.Div([
                html.Span(f"{total_count:,} ", style={"fontWeight": "bold", "color": "#FFFFFF"}),
                html.Span(f"propert{'ies' if total_count != 1 else 'y'} affected", style={"color": "#CCCCCC"})
            ], style={"marginBottom": "5px"}),
            html.Div([
                html.Span("Total affected property value: ", style={"color": "#CCCCCC"}),
                html.Span(f"${total_value:,.2f}", style={"fontWeight": "bold", "color": "#4CAF50"})
            ], style={"marginBottom": "5px"}),
            html.Div([
                html.Span("Total proposed payout: ", style={"color": "#CCCCCC"}),
                html.Span(f"${total_payout:,.2f}", style={"fontWeight": "bold", "color": "#4CAF50"})
            ])
        ])
        
        # Add new property markers with dl.Marker for better interactivity
        property_markers = []
        for idx, row in properties_df.iterrows():
            marker = dl.Marker(
                id={"type": "property-marker", "index": str(row['property_id'])},
                position=[row['lat'], row['lon']],
                n_clicks=0,
                children=[
                    dl.Tooltip(
                        children=html.Div([
                            html.Strong(f"Portfolio ID: {row['portfolio_id']}", style={"display": "block", "marginBottom": "5px"}),
                            html.Strong(f"Property ID: {row['property_id']}", style={"display": "block", "marginBottom": "5px"}),
                            html.Span(f"Value: ${row['property_value']:,.2f}" if pd.notna(row['property_value']) else "Value: N/A", 
                                     style={"display": "block", "marginBottom": "3px"}),
                            html.Span(str(row['name']) if pd.notna(row['name']) else "", 
                                     style={"display": "block", "fontSize": "11px"})
                        ])
                    )
                ],
                # className="property-marker"
            )
            property_markers.append(marker)
        print(f"Time taken to create property markers: {dt.datetime.now() - stime}")
        
        # Combine existing elements with event polygon and new markers
        # updated_children = filtered_children + [event_polygon_element] + property_markers
        # updated_children = updated_children + property_markers
        
        print(f'Time taken to combine elements, {len(updated_hex_children)} children: {dt.datetime.now() - stime}')
        return viewport, html.Div(property_cards), agg_div, property_data, event_polygons_children, property_markers, updated_hex_children, 'all'
            
    except Exception as e:
        print(f"Error fetching properties: {e}")
        return viewport, html.Div(
            f"Error loading properties: {str(e)}",
            style={
                "color": "#FF6B6B",
                "fontFamily": "Helvetica",
                "fontSize": "14px",
                "textAlign": "center",
                "padding": "20px"
            }
        ), html.Div("Error"), [], [], [], hex_children, 'all'

@app.callback(
    # Output('clicked-property', 'data'),
    [Output({"type": "property-card", "index": dash.dependencies.ALL}, "style"),
     Output({"type": "property-marker", "index": dash.dependencies.ALL}, "icon")],
    [Input({"type": "property-card", "index": dash.dependencies.ALL}, "n_clicks"),
     Input({"type": "property-marker", "index": dash.dependencies.ALL}, "n_clicks")],
    [State({"type": "property-card", "index": dash.dependencies.ALL}, "id"),
     State({"type": "property-marker", "index": dash.dependencies.ALL}, "id"),
     State('property-data-store', 'data'),
     State('clicked-property', 'data')],
    prevent_initial_call=True
)
def handle_property_click(card_clicks, marker_clicks, card_ids, marker_ids, property_data, current_clicked):
    """Handle clicks on property cards or markers - only updates highlighting, no data reload"""
    print("PROPERTY CLICK CALLBACK")
    
    ctx = callback_context
    
    if not ctx.triggered:
        return no_update, no_update
    
    # print("triggered:", ctx.triggered)

    triggered_id = ctx.triggered[0]['prop_id']
    triggered_dict = json.loads(triggered_id.split('.')[0])
    property_id = triggered_dict['index']
    triggered_card_ids = [str(x['index']) for x in card_ids]
    core = [x['core_polygon'] for x in property_data if str(x['property_id']) in triggered_card_ids]
    secondary = [x['secondary_polygon'] for x in property_data if str(x['property_id']) in triggered_card_ids]

    global icon_style_core, icon_style_secondary, icon_style_clicked
    global card_style_core, card_style_secondary, card_style_clicked

    triggered_dict = json.loads(triggered_id.split('.')[0])
    property_id = triggered_dict['index']
    
    card_styles = []
    marker_icons = []

    # print(len(card_ids), len(triggered_card_ids), len(core), len(secondary))
    for i in range(len(card_ids)):
        if card_ids[i]["index"] == property_id:
            card_style = card_style_clicked
            marker_icon = icon_style_clicked
        elif core[i]:
            card_style = card_style_core
            marker_icon = icon_style_core
        elif secondary[i]:
            card_style = card_style_secondary
            marker_icon = icon_style_secondary
        card_styles.append(card_style)
        marker_icons.append(marker_icon)

    return card_styles, marker_icons

# Callback to filter property cards and markers by portfolio ID
@app.callback(
    [Output('property-list', 'children', allow_duplicate=True),
     Output('property-agg', 'children', allow_duplicate=True),
     Output('map-property-markers', 'children', allow_duplicate=True)],
    [Input('portfolio-dropdown', 'value')],
    [State('property-data-store', 'data'),
     State('event-dropdown', 'value'),
     State('map-property-markers', 'children')],
    prevent_initial_call=True
)
def filter_properties_by_portfolio(selected_portfolio_id, property_data, event_name, children):
    """Filter property cards and markers based on selected portfolio ID"""
    
    if not property_data:
        return no_update, no_update, no_update
    
    # If no portfolio is selected (cleared) or "all" is selected, show all properties
    if selected_portfolio_id is None or selected_portfolio_id == 'all':
        properties_df = pd.DataFrame(property_data)
    else:
        # Filter properties by portfolio_id
        properties_df = pd.DataFrame([prop for prop in property_data if prop['portfolio_id'] == selected_portfolio_id])
    
    if properties_df.empty:
        return html.Div(
            "No properties found for this portfolio",
            style={
                "color": "#CCCCCC",
                "fontFamily": "Helvetica",
                "fontSize": "14px",
                "textAlign": "center",
                "padding": "20px"
            }
        ), html.Div("0 properties"), children
    
    global card_style_core, card_style_secondary, card_style_clicked
    global icon_style_core, icon_style_secondary, icon_style_clicked
    # Create property cards with the same styling logic as original
    property_cards = []
    for idx, row in properties_df.iterrows():
        card = html.Div(
            [
                html.Button(
                    [
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Strong("Portfolio ID: ", style={"color": "#FFFFFF"}),
                                        html.Span(str(row['portfolio_id']), style={"color": "#CCCCCC"})
                                    ],
                                    style={"marginBottom": "5px"}
                                ),
                                html.Div(
                                    [
                                        html.Strong("Property ID: ", style={"color": "#FFFFFF"}),
                                        html.Span(str(row['property_id']), style={"color": "#CCCCCC"})
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
                                    style={"marginBottom": "10px"}
                                ),
                                html.Button(
                                    "📊 Property Analysis",
                                    id={"type": "property-analysis-btn", "index": str(row['property_id'])},
                                    n_clicks=0,
                                    style={
                                        "backgroundColor": "#4CAF50",
                                        "color": "#FFFFFF",
                                        "padding": "8px 16px",
                                        "border": "1px solid #4CAF50",
                                        "borderRadius": "4px",
                                        "fontFamily": "Helvetica",
                                        "fontSize": "11px",
                                        "fontWeight": "bold",
                                        "cursor": "pointer",
                                        "width": "100%",
                                        "textAlign": "center",
                                        "transition": "background-color 0.2s ease",
                                    }
                                )
                            ]
                        )
                    ],
                    id={"type": "property-card", "index": str(row['property_id'])},
                    n_clicks=0,
                    className="property-card",
                    style=card_style_core if row['core_polygon'] else card_style_secondary
                )
            ],
            style={"marginBottom": "10px"}
        )
        property_cards.append(card)
    
    # Update aggregate statistics
    total_count = len(properties_df)
    total_value = properties_df['property_value'].sum()
    total_payout = properties_df['property_payout'].sum()
    
    agg_div = html.Div([
        html.Div([
            html.Span(f"{total_count:,} ", style={"fontWeight": "bold", "color": "#FFFFFF"}),
            html.Span(f"propert{'ies' if total_count != 1 else 'y'} affected", style={"color": "#CCCCCC"})
        ], style={"marginBottom": "5px"}),
        html.Div([
            html.Span("Total affected property value: ", style={"color": "#CCCCCC"}),
            html.Span(f"${total_value:,.2f}", style={"fontWeight": "bold", "color": "#4CAF50"})
        ], style={"marginBottom": "5px"}),
        html.Div([
            html.Span("Total proposed payout: ", style={"color": "#CCCCCC"}),
            html.Span(f"${total_payout:,.2f}", style={"fontWeight": "bold", "color": "#4CAF50"})
        ])
    ])
    
    # Filter markers to only show properties in the selected portfolio
    # Remove old property markers and add new filtered ones
    # filtered_children = [x for x in children 
    #                     if 'id' in x.get('props', {}) 
    #                     and (isinstance(x['props']['id'], dict) 
    #                          and x['props']['id'].get('type') != 'property-marker')]
    
    # Create new property markers for filtered properties
    property_markers = []
    for idx, row in properties_df.iterrows():
        # print(row['property_id'], row['housenumber'], row['street'], ',', row['city'], ',', row['state'], ',', row['postcode'])
        marker = dl.Marker(
            id={"type": "property-marker", "index": str(row['property_id'])},
            position=[row['lat'], row['lon']],
            n_clicks=0,
            children=[
                dl.Tooltip(
                    children=html.Div([
                        html.Strong(f"Portfolio ID: {row['portfolio_id']}", style={"display": "block", "marginBottom": "5px"}),
                        html.Strong(f"Property ID: {row['property_id']}", style={"display": "block", "marginBottom": "5px"}),
                        html.Span(f"Value: ${row['property_value']:,.2f}" if pd.notna(row['property_value']) else "Value: N/A", 
                                 style={"display": "block", "marginBottom": "3px"}),
                        html.Span(str(row['name']) if pd.notna(row['name']) else "", 
                                 style={"display": "block", "fontSize": "11px"})
                    ])
                )
            ],
            icon=icon_style_core if row['core_polygon'] else icon_style_secondary
        )
        property_markers.append(marker)
    
    # updated_children = filtered_children + property_markers
    
    return html.Div(property_cards), agg_div, property_markers
    
# # Callback to start iterative text update
@app.callback(
    [Output("interval-component", "disabled", allow_duplicate=True),
     Output('active-ai-assessment', 'data')],
    [Input({"type": "ai-assessment-btn", "index": dash.dependencies.ALL}, "n_clicks")],
    [State('property-data-store', 'data'),
     State('event-dropdown', 'value')],
    prevent_initial_call=True
)
def start_text_update(btn_clicks, property_data, event_name):
    print(f"START TEXT UPDATE") # {btn_clicks} clicks")

    if max(btn_clicks) == 0:
        return True, None

    ctx = callback_context
    
    if not ctx.triggered:
        return True, None
    
    triggered_id = ctx.triggered[0]['prop_id']
    triggered_dict = json.loads(triggered_id.split('.')[0])
    property_id = triggered_dict['index']

    if property_data:
        clicked_property_data = [x for x in property_data if str(x['property_id']) == str(property_id)][0]
        model = os.getenv("LLM_ENDPOINT")
        threading.Thread(target=fmapi_stream_ai_assessment, args=(get_databricks_sp_token(), get_databricks_server_hostname(), model, clicked_property_data, event_name,)).start()
        print("Turning on the interval-component")
        return False, property_id
    else:
        return True, None

@app.callback(
    [Output({"type": "ai-assessment-text", "index": dash.dependencies.ALL}, "children"),
     Output({"type": "ai-assessment-text", "index": dash.dependencies.ALL}, "style"),
     Output("interval-component", "disabled", allow_duplicate=True)],
    [Input("interval-component", "n_intervals")],
    [State('active-ai-assessment', 'data'),
     State({"type": "ai-assessment-text", "index": dash.dependencies.ALL}, "id")],
    prevent_initial_call=True,
)
def update_response(n_intervals, active_property_id, text_ids):
    global response_list
    global stream_complete

    if not active_property_id or not text_ids:
        return [no_update] * len(text_ids), [no_update] * len(text_ids), True

    # Initialize outputs
    children_outputs = [no_update] * len(text_ids)
    style_outputs = [no_update] * len(text_ids)
    
    # Find the index of the active property
    active_index = None
    for i, text_id in enumerate(text_ids):
        if str(text_id['index']) == str(active_property_id):
            active_index = i
            break
    
    if active_index is None:
        return children_outputs, style_outputs, True

    visible_style = {
        "display": "block",
        "marginTop": "10px",
        "padding": "10px",
        "backgroundColor": "#1A1A1A",
        "borderRadius": "4px",
        "border": "1px solid #5A5A5A",
        "color": "#FFFFFF",
        "fontFamily": "Helvetica",
        "fontSize": "11px",
        "lineHeight": "1.5",
        "whiteSpace": "pre-wrap"
    }

    if not stream_complete:
        # Stream is in process
        current_text = "".join([x for x in response_list if x is not None])
        children_outputs[active_index] = current_text
        style_outputs[active_index] = visible_style
        return children_outputs, style_outputs, False
    else:
        # print("STREAM IS DONE")
        if len(response_list) > 0:
            final_results = [x for x in response_list if x is not None]
            children_outputs[active_index] = "".join(final_results)
            style_outputs[active_index] = visible_style
            response_list = []
            return children_outputs, style_outputs, True
        return children_outputs, style_outputs, True

# Callback to start draft message text update
@app.callback(
    [Output("interval-component-draft", "disabled", allow_duplicate=True),
     Output('active-draft-message', 'data')],
    [Input({"type": "draft-message-btn", "index": dash.dependencies.ALL}, "n_clicks")],
    [State('property-data-store', 'data'),
     State('event-dropdown', 'value')],
    prevent_initial_call=True
)
def start_draft_message_update(btn_clicks, property_data, event_name):
    print(f"START DRAFT MESSAGE UPDATE")

    if max(btn_clicks) == 0:
        return True, None

    ctx = callback_context
    
    if not ctx.triggered:
        return True, None
    
    triggered_id = ctx.triggered[0]['prop_id']
    triggered_dict = json.loads(triggered_id.split('.')[0])
    property_id = triggered_dict['index']

    if property_data:
        clicked_property_data = [x for x in property_data if str(x['property_id']) == str(property_id)][0]
        model = os.getenv("LLM_ENDPOINT")
        threading.Thread(target=fmapi_stream_draft_message, args=(get_databricks_sp_token(), get_databricks_server_hostname(), model, clicked_property_data, event_name,)).start()
        print("Turning on the interval-component-draft")
        return False, property_id
    else:
        return True, None

@app.callback(
    [Output({"type": "draft-message-text", "index": dash.dependencies.ALL}, "children"),
     Output({"type": "draft-message-text", "index": dash.dependencies.ALL}, "style"),
     Output("interval-component-draft", "disabled", allow_duplicate=True)],
    [Input("interval-component-draft", "n_intervals")],
    [State('active-draft-message', 'data'),
     State({"type": "draft-message-text", "index": dash.dependencies.ALL}, "id")],
    prevent_initial_call=True,
)
def update_draft_message_response(n_intervals, active_property_id, text_ids):
    global draft_message_response_list
    global draft_message_stream_complete

    if not active_property_id or not text_ids:
        return [no_update] * len(text_ids), [no_update] * len(text_ids), True

    # Initialize outputs
    children_outputs = [no_update] * len(text_ids)
    style_outputs = [no_update] * len(text_ids)
    
    # Find the index of the active property
    active_index = None
    for i, text_id in enumerate(text_ids):
        if str(text_id['index']) == str(active_property_id):
            active_index = i
            break
    
    if active_index is None:
        return children_outputs, style_outputs, True

    visible_style = {
        "display": "block",
        "marginTop": "10px",
        "padding": "10px",
        "backgroundColor": "#1A1A1A",
        "borderRadius": "4px",
        "border": "1px solid #5A5A5A",
        "color": "#FFFFFF",
        "fontFamily": "Helvetica",
        "fontSize": "11px",
        "lineHeight": "1.5",
        "whiteSpace": "pre-wrap"
    }

    if not draft_message_stream_complete:
        # Stream is in process
        current_text = "".join([x for x in draft_message_response_list if x is not None])
        children_outputs[active_index] = current_text
        style_outputs[active_index] = visible_style
        return children_outputs, style_outputs, False
    else:
        # print("DRAFT MESSAGE STREAM IS DONE")
        if len(draft_message_response_list) > 0:
            final_results = [x for x in draft_message_response_list if x is not None]
            children_outputs[active_index] = "".join(final_results)
            style_outputs[active_index] = visible_style
            draft_message_response_list = []
            return children_outputs, style_outputs, True
        return children_outputs, style_outputs, True

# Callback to show/hide property analysis overlay and store selected property
@app.callback(
    [Output('portfolio-analysis-overlay', 'style'),
     Output('clicked-property', 'data'),
     Output('dashboard-ai-assessment-text', 'children', allow_duplicate=True),
     Output('dashboard-ai-assessment-text', 'style', allow_duplicate=True),
     Output('dashboard-draft-email-text', 'children', allow_duplicate=True),
     Output('dashboard-draft-email-text', 'style', allow_duplicate=True),
     Output('property-image-original-container', 'children', allow_duplicate=True),
     Output('property-image-damaged-container', 'children', allow_duplicate=True),
     Output('dashboard-iframe-overlay', 'src', allow_duplicate=True)],
    [Input({'type': 'property-analysis-btn', 'index': ALL}, 'n_clicks'),
     Input('close-portfolio-analysis-btn', 'n_clicks')],
    [State('portfolio-analysis-overlay', 'style')],
    prevent_initial_call=True
)
def toggle_property_analysis_overlay(property_btn_clicks, close_clicks, current_style):
    """Toggle the property analysis overlay visibility and store selected property"""
    ctx = callback_context
    
    if not ctx.triggered:
        return no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update
    
    triggered_prop = ctx.triggered[0]['prop_id']
    
    # Visible style
    visible_style = {
        "display": "flex",
        "position": "fixed",
        "top": "0",
        "left": "0",
        "width": "100%",
        "height": "100%",
        "backgroundColor": "rgba(0, 0, 0, 0.8)",
        "zIndex": "10000",
        "justifyContent": "center",
        "alignItems": "center"
    }
    
    # Hidden style
    hidden_style = {
        "display": "none",
        "position": "fixed",
        "top": "0",
        "left": "0",
        "width": "100%",
        "height": "100%",
        "backgroundColor": "rgba(0, 0, 0, 0.8)",
        "zIndex": "10000",
        "justifyContent": "center",
        "alignItems": "center"
    }
    
    # Hidden style for text elements
    hidden_text_style = {
        "display": "none",
        "padding": "10px",
        "backgroundColor": "#1A1A1A",
        "borderRadius": "4px",
        "border": "1px solid #5A5A5A",
        "color": "#FFFFFF",
        "fontFamily": "Helvetica",
        "fontSize": "12px",
        "lineHeight": "1.5",
        "whiteSpace": "pre-wrap"
    }
    
    # Check if a property analysis button was clicked
    if 'property-analysis-btn' in triggered_prop:
        # Make sure a button was actually clicked, not just created
        if property_btn_clicks and max(property_btn_clicks) > 0:
            # Extract the property ID from the triggered button
            import json
            # Parse the triggered_prop to extract property ID
            # Format is like: '{"index":"123","type":"property-analysis-btn"}.n_clicks'
            button_id_str = triggered_prop.split('.')[0]
            button_id = json.loads(button_id_str)
            property_id = button_id['index']
            # Return visible style, property ID, and clear all content (including iframe)
            return visible_style, property_id, "", hidden_text_style, "", hidden_text_style, [], [], ""
        else:
            # Buttons were just created, don't open overlay
            return no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update
    elif 'close-portfolio-analysis-btn' in triggered_prop:
        return hidden_style, no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update
    
    return no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update

# Callback to update property images (original and damaged) based on selected property
@app.callback(
    [Output('property-image-original-container', 'children'),
     Output('property-image-damaged-container', 'children')],
    [Input('clicked-property', 'data')],
    [State('property-data-store', 'data')],
    prevent_initial_call=True
)
def update_property_images(property_id, property_data):
    """Update property images (original and damaged) based on selected property"""
    no_image_text = html.Div(
        "No image available.",
        style={
            "color": "#999999",
            "fontFamily": "Helvetica",
            "fontSize": "12px",
            "fontStyle": "italic",
            "textAlign": "center"
        }
    )
    
    if not property_id or not property_data:
        return no_image_text, no_image_text
    
    # Find the clicked property data
    clicked_property_data = None
    for prop in property_data:
        if str(prop['property_id']) == str(property_id):
            clicked_property_data = prop
            break
    
    if not clicked_property_data:
        return no_image_text, no_image_text
    
    # Construct file paths for both images
    original_image_path = f"/assets/images/{property_id}.png"
    damaged_image_path = f"/assets/images/{property_id}_damaged.png"
    
    # Check if images exist on the file system
    app_directory = os.path.dirname(os.path.abspath(__file__))
    original_file_exists = os.path.exists(os.path.join(app_directory, f"assets/images/{property_id}.png"))
    damaged_file_exists = os.path.exists(os.path.join(app_directory, f"assets/images/{property_id}_damaged.png"))

    # Create original image component or placeholder
    if original_file_exists:
        original_content = html.Img(
            src=original_image_path,
            style={
                "width": "100%",
                "borderRadius": "4px",
                "border": "1px solid #5A5A5A"
            }
        )
    else:
        import random
        images_dir = os.path.join(app_directory, "assets/images")
        all_image_files = [f for f in os.listdir(images_dir) if f.endswith(".png") and not f.endswith("_damaged.png")]
        if all_image_files:
            random_original_file = random.choice(all_image_files)
            random_original_image_path = f"/assets/images/{random_original_file}"
            original_content = html.Img(
                src=random_original_image_path,
                style={
                    "width": "100%",
                    "borderRadius": "4px",
                    "border": "1px solid #5A5A5A"
                }
            )
        else:
            original_content = no_image_text
    
    # Create damaged image component or placeholder
    if damaged_file_exists:
        damaged_content = html.Img(
            src=damaged_image_path,
            style={
                "width": "100%",
                "borderRadius": "4px",
                "border": "1px solid #5A5A5A"
            }
        )
    else:
        if random_original_image_path:
            random_damaged_image_path = random_original_image_path.replace(".png", "_damaged.png")
            damaged_content = html.Img(
                src=random_damaged_image_path,
                style={
                    "width": "100%",
                    "borderRadius": "4px",
                    "border": "1px solid #5A5A5A"
                }
            )
        else:
            damaged_content = no_image_text
    
    return original_content, damaged_content

# Callback to update dashboard iframe URL with portfolio-id and property-id parameters
@app.callback(
    Output('dashboard-iframe-overlay', 'src'),
    [Input('clicked-property', 'data')],
    [State('property-data-store', 'data')],
    prevent_initial_call=True
)
def update_dashboard_iframe_url(property_id, property_data):
    """Update dashboard iframe URL with portfolio-id and property-id query parameters"""
    if not property_id or not property_data or not DASHBOARD_URL:
        return ""
    
    # Find the clicked property data
    clicked_property_data = None
    for prop in property_data:
        if str(prop['property_id']) == str(property_id):
            clicked_property_data = prop
            break
    
    if not clicked_property_data:
        return ""
    
    # Extract portfolio_id and property_id
    portfolio_id = clicked_property_data.get('portfolio_id', '')
    
    # Build the URL with query parameters
    # Format: &f_a6b2965b~portfolio-id=1&f_a6b2965b~property-id-1=314115730
    url_params = f"&f_a6b2965b~portfolio-id={portfolio_id}&f_a6b2965b~property-id-1={property_id}"
    
    # Append to the base dashboard URL
    if '?' in DASHBOARD_URL:
        full_url = f"{DASHBOARD_URL}{url_params}"
    else:
        full_url = f"{DASHBOARD_URL}?{url_params[1:]}"  # Remove leading &
    
    return full_url

# Callback to start dashboard AI Assessment streaming
@app.callback(
    [Output("dashboard-interval-ai", "disabled"),
     Output("dashboard-ai-assessment-btn", "disabled")],
    [Input('dashboard-ai-assessment-btn', 'n_clicks')],
    [State('clicked-property', 'data'),
     State('property-data-store', 'data'),
     State('event-dropdown', 'value')],
    prevent_initial_call=True
)
def start_dashboard_ai_assessment(n_clicks, property_id, property_data, event_name):
    """Start streaming AI Assessment for dashboard view"""
    if not n_clicks or not property_id or not property_data:
        return True, False
    
    # Find the clicked property data
    clicked_property_data = None
    for prop in property_data:
        if str(prop['property_id']) == str(property_id):
            clicked_property_data = prop
            break
    
    if not clicked_property_data:
        return True, False
    
    # Start streaming in a thread
    model = os.getenv("LLM_ENDPOINT")
    threading.Thread(target=fmapi_stream_ai_assessment, args=(get_databricks_sp_token(), get_databricks_server_hostname(), model, clicked_property_data, event_name,)).start()
    print("Starting dashboard AI assessment stream")
    return False, True  # Enable interval, disable button

# Callback to update dashboard AI Assessment text from stream
@app.callback(
    [Output('dashboard-ai-assessment-text', 'children'),
     Output('dashboard-ai-assessment-text', 'style'),
     Output("dashboard-interval-ai", "disabled", allow_duplicate=True),
     Output("dashboard-ai-assessment-btn", "disabled", allow_duplicate=True)],
    [Input("dashboard-interval-ai", "n_intervals")],
    prevent_initial_call=True
)
def update_dashboard_ai_assessment(n_intervals):
    """Update dashboard AI Assessment text from stream"""
    global response_list
    global stream_complete
    
    visible_style = {
        "display": "block",
        "padding": "10px",
        "backgroundColor": "#1A1A1A",
        "borderRadius": "4px",
        "border": "1px solid #5A5A5A",
        "color": "#FFFFFF",
        "fontFamily": "Helvetica",
        "fontSize": "12px",
        "lineHeight": "1.5",
        "whiteSpace": "pre-wrap"
    }

    # print("RESPONSE LIST:", response_list)
    # print("STREAM COMPLETE:", stream_complete)
    if not stream_complete:
        # Stream is in progress
        current_text = "".join([x for x in response_list if x is not None and isinstance(x, str)])
        return current_text, visible_style, False, True  # Keep interval enabled, button disabled
    else:
        # Stream is complete
        final_text = "".join([x for x in response_list if x is not None and isinstance(x, str)])
        return final_text, visible_style, True, False  # Disable interval, enable button

# Callback to start dashboard Draft Email streaming
@app.callback(
    [Output("dashboard-interval-email", "disabled"),
     Output("dashboard-draft-email-btn", "disabled")],
    [Input('dashboard-draft-email-btn', 'n_clicks')],
    [State('clicked-property', 'data'),
     State('property-data-store', 'data'),
     State('event-dropdown', 'value')],
    prevent_initial_call=True
)
def start_dashboard_draft_email(n_clicks, property_id, property_data, event_name):
    """Start streaming Draft Email for dashboard view"""
    if not n_clicks or not property_id or not property_data:
        return True, False
    
    # Find the clicked property data
    clicked_property_data = None
    for prop in property_data:
        if str(prop['property_id']) == str(property_id):
            clicked_property_data = prop
            break
    
    if not clicked_property_data:
        return True, False
    
    # Start streaming in a thread
    model = os.getenv("LLM_ENDPOINT")
    threading.Thread(target=fmapi_stream_draft_message, args=(get_databricks_sp_token(), get_databricks_server_hostname(), model, clicked_property_data, event_name,)).start()
    print("Starting dashboard draft email stream")
    return False, True  # Enable interval, disable button

# Callback to update dashboard Draft Email text from stream
@app.callback(
    [Output('dashboard-draft-email-text', 'children'),
     Output('dashboard-draft-email-text', 'style'),
     Output("dashboard-interval-email", "disabled", allow_duplicate=True),
     Output("dashboard-draft-email-btn", "disabled", allow_duplicate=True)],
    [Input("dashboard-interval-email", "n_intervals")],
    prevent_initial_call=True
)
def update_dashboard_draft_email(n_intervals):
    """Update dashboard Draft Email text from stream"""
    global draft_message_response_list
    global draft_message_stream_complete
    
    visible_style = {
        "display": "block",
        "padding": "10px",
        "backgroundColor": "#1A1A1A",
        "borderRadius": "4px",
        "border": "1px solid #5A5A5A",
        "color": "#FFFFFF",
        "fontFamily": "Helvetica",
        "fontSize": "12px",
        "lineHeight": "1.5",
        "whiteSpace": "pre-wrap"
    }
    
    if not draft_message_stream_complete:
        # Stream is in progress
        current_text = "".join([x for x in draft_message_response_list if x is not None and isinstance(x, str)])
        return current_text, visible_style, False, True  # Keep interval enabled, button disabled
    else:
        # Stream is complete
        final_text = "".join([x for x in draft_message_response_list if x is not None and isinstance(x, str)])
        return final_text, visible_style, True, False  # Disable interval, enable button

# Navigation callbacks
@app.callback(
    Output('nav-collapsed', 'data'),
    Input('nav-toggle', 'n_clicks'),
    State('nav-collapsed', 'data'),
    prevent_initial_call=True
)
def toggle_nav(n_clicks, is_collapsed):
    """Toggle navigation menu collapsed state"""
    return not is_collapsed

@app.callback(
    [Output('nav-menu', 'style'),
     Output('map-content', 'style'),
     Output('dashboard-content', 'style', allow_duplicate=True)],
    Input('nav-collapsed', 'data'),
    State('active-page', 'data'),
    prevent_initial_call='initial_duplicate'
)
def update_nav_style(is_collapsed, active_page):
    """Update navigation menu and content styles based on collapsed state"""
    if is_collapsed:
        nav_style = {
            "position": "fixed",
            "left": "0",
            "top": "0",
            "height": "100vh",
            "width": "60px",
            "backgroundColor": "#2C2C2C",
            "zIndex": "10001",
            "transition": "width 0.3s ease",
            "boxShadow": "2px 0 5px rgba(0,0,0,0.3)",
            "fontFamily": "Helvetica",
            "overflow": "hidden"
        }
        map_style = {
            "marginLeft": "60px",
            "transition": "margin-left 0.3s ease",
            "width": "calc(100% - 60px)",
            "display": "block" if active_page == 'map' else "none"
        }
        dashboard_style = {
            "display": "block" if active_page == 'dashboard' else "none",
            "marginLeft": "60px",
            "transition": "margin-left 0.3s ease",
            "width": "calc(100% - 60px)"
        }
    else:
        nav_style = {
            "position": "fixed",
            "left": "0",
            "top": "0",
            "height": "100vh",
            "width": "250px",
            "backgroundColor": "#2C2C2C",
            "zIndex": "10001",
            "transition": "width 0.3s ease",
            "boxShadow": "2px 0 5px rgba(0,0,0,0.3)",
            "fontFamily": "Helvetica"
        }
        map_style = {
            "marginLeft": "250px",
            "transition": "margin-left 0.3s ease",
            "width": "calc(100% - 250px)",
            "display": "block" if active_page == 'map' else "none"
        }
        dashboard_style = {
            "display": "block" if active_page == 'dashboard' else "none",
            "marginLeft": "250px",
            "transition": "margin-left 0.3s ease",
            "width": "calc(100% - 250px)"
        }
    
    return nav_style, map_style, dashboard_style

@app.callback(
    Output('active-page', 'data'),
    [Input('map-nav-item', 'n_clicks'),
     Input('dashboard-nav-item', 'n_clicks')],
    prevent_initial_call=True
)
def switch_page(map_clicks, dashboard_clicks):
    """Switch between map and dashboard pages"""
    ctx = callback_context
    if not ctx.triggered:
        return 'map'
    
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    if button_id == 'map-nav-item':
        return 'map'
    elif button_id == 'dashboard-nav-item':
        return 'dashboard'
    
    return 'map'

@app.callback(
    [Output('map-content', 'style', allow_duplicate=True),
     Output('dashboard-content', 'style', allow_duplicate=True),
     Output('map-nav-item', 'style'),
     Output('dashboard-nav-item', 'style'),
     Output('map-nav-text', 'style'),
     Output('dashboard-nav-text', 'style')],
    [Input('active-page', 'data'),
     Input('nav-collapsed', 'data')],
    prevent_initial_call='initial_duplicate'
)
def update_page_display(active_page, is_collapsed):
    """Update page display and navigation highlighting based on active page"""
    margin = "60px" if is_collapsed else "250px"
    width = f"calc(100% - {margin})"
    
    # Content styles
    map_style = {
        "display": "block" if active_page == 'map' else "none",
        "marginLeft": margin,
        "transition": "margin-left 0.3s ease",
        "width": width
    }
    dashboard_style = {
        "display": "block" if active_page == 'dashboard' else "none",
        "marginLeft": margin,
        "transition": "margin-left 0.3s ease",
        "width": width
    }
    
    # Navigation item styles
    active_nav_style = {
        "padding": "15px",
        "cursor": "pointer",
        "color": "#FFFFFF",
        "backgroundColor": "#4A4A4A",
        "borderBottom": "1px solid #3A3A3A",
        "display": "flex",
        "alignItems": "center",
        "borderLeft": "4px solid #4CAF50"
    }
    inactive_nav_style = {
        "padding": "15px",
        "cursor": "pointer",
        "color": "#FFFFFF",
        "backgroundColor": "#3A3A3A",
        "borderBottom": "1px solid #3A3A3A",
        "display": "flex",
        "alignItems": "center",
        "borderLeft": "4px solid transparent"
    }
    
    # Text styles for collapsed state
    text_style_visible = {"display": "inline"}
    text_style_hidden = {"display": "none" if is_collapsed else "inline"}
    
    if active_page == 'map':
        return map_style, dashboard_style, active_nav_style, inactive_nav_style, text_style_hidden, text_style_hidden
    else:
        return map_style, dashboard_style, inactive_nav_style, active_nav_style, text_style_hidden, text_style_hidden

if __name__ == "__main__":
    app.run(debug=True)