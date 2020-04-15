import os
from datetime import datetime
import json
import pandas as pd

import dash
import dash_html_components as html
import dash_core_components as dcc
import dash_table

from dash.dependencies import Input, Output

from flask import Flask, request, jsonify, Response, abort

import requests
from requests.exceptions import Timeout

from sesamutils import sesam_logger, VariablesConfig
from sesamutils.flask import serve

# Default values can be given to optional environment variables by the use of tuples
required_env_vars = ["JWT", "HUBNR","SYS_ID"]
optional_env_vars = [("LOG_LEVEL", "INFO"),("LOCAL_DEV","False")] 
config = VariablesConfig(required_env_vars, optional_env_vars=optional_env_vars)

SYS_ID = config.SYS_ID
PROXY = '/'
if config.LOCAL_DEV == 'True':
    PROXY = '/'
else:
    PROXY = f'/api/systems/{SYS_ID}/proxy/'
JWT = config.JWT
URL = f'https://datahub-{config.HUBNR}.sesam.cloud/api/'
HEADER = {'Authorization': f'Bearer {JWT}'}

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets,requests_pathname_prefix=PROXY,
                routes_pathname_prefix='/')

server = app.server

logger = sesam_logger('insight', app=server, timestamp=True)
    
if not config.validate():
    logger.error("Environment variables do not validate. Exiting system.")
    os.sys.exit(1)

app.layout = html.Div([
    dcc.Tabs(id="tabs", value='node', children=[
        dcc.Tab(label='Node info', value='node'),
        dcc.Tab(label='Systems', value='systems'),
        dcc.Tab(label='Pipes', value='pipes'),
        dcc.Tab(label='Entities', value='entities')
    ]),
    html.Div(id='tabs-content')
])

def getNodeInfo():
    resp_api = requests.get(URL)
    return resp_api.json()

def getSystemsData():
    url_systems = URL+f"systems"
    resp_systems = requests.get(url_systems,headers=HEADER)
    return pd.json_normalize(resp_systems.json())

def getPipesData():
    url_pipes = URL+f"pipes"
    resp_pipes = requests.get(url_pipes,headers=HEADER)
    return pd.json_normalize(resp_pipes.json())

def getEntities(pipeId):
    url_entities = URL+f"pipes/{pipeId}/entities?limit=10"
    resp_entities = requests.get(url_entities,headers=HEADER)
    return pd.json_normalize(resp_entities.json())

def generateTable(df):
    return dash_table.DataTable(style_table={'maxHeight': '300px','overflowY': 'scroll'},
                    style_cell={'textAlign': 'left'},
                    id='table',
                    columns=[{"name": f'{i} ({j})', "id": i} for i,j in zip(df.columns, df.dtypes)],
                    data=df.to_dict('records'))

def cell_len(x):
    if isinstance(x,str) or isinstance(x,list):
        return len(str(x))
    return x

def stream_as_json(generator_function):
    """Helper generator to support streaming with flask to Sesam"""
    first = True
    yield '['
    for item in generator_function:
        if not first:
            yield ','
        else:
            first = False
        yield json.dumps(item)
    yield ']'

@server.route("/test/",methods=["GET"]) 
def get_url():
    try:
        if request.args.get('since') is None:
            logger.debug(f"since value not set. "+str(request.url))
        else:
            unix_time_update_date = request.args.get('since')
            logger.debug(f"since value sent from sesam: {unix_time_update_date}")
        with requests.Session() as session: # For streaming with generator
            session.headers.update(HEADER)
            logger.debug("URL: "+URL)
            resp = session.get(URL, timeout=180)
            data = [{"data": resp.json()}]
            logger.debug(resp.json())
            return Response(json.dumps(data), mimetype='application/json; charset=utf-8')
            # return Response(stream_as_json(data), mimetype='application/json; charset=utf-8')
    except Timeout as e:
        logger.error(f"Timeout issue while fetching {e}")
    except ConnectionError as e:
        logger.error(f"ConnectionError issue while fetching {e}")
    except Exception as e:
        logger.error(f"Issue while fetching {e}")

@server.route("/stats/",methods=["POST"]) 
def post_stats():
    payload = request.json
    data = pd.json_normalize(payload)
    dataColumns = list(data.columns)
    map_tmp = list(map(lambda s:list(str.split(s,'.')),dataColumns))
    map_tmp = pd.DataFrame(map_tmp).fillna('')
    data.columns = pd.MultiIndex.from_frame(map_tmp)
    data_stat = data.applymap(cell_len).describe(include="all").T
    data_stat['type'] =[type(c).__name__ for c in data.iloc[0]]
    logger.debug(f'post_stats: {data_stat.to_json(orient="index")}')
    return Response('[\n'+data_stat.to_json(orient="index")+'\n]', mimetype='application/json; charset=utf-8')

@server.route("/flatten/",methods=["POST"]) 
def post_flatten():
    payload = request.json
    data = pd.json_normalize(payload,sep='~')
    return Response(data.to_json(orient="records"), mimetype='application/json; charset=utf-8')

@app.callback(Output('tabs-content', 'children'),[Input('tabs', 'value')])
def render_content(tab):
    data = list()
    if tab == "node":
        node_data = getNodeInfo()
        return html.Div([
            html.Pre(json.dumps(node_data,indent=4))
                        ])
    if tab == "systems":
        systems_data = getSystemsData()
        systems_count = systems_data.groupby("config.effective.type")['_id'].count()
        data = [{'labels': list(systems_count.index),'values': list(systems_count),'type': 'pie'}]
        return html.Div([
            dcc.Graph(id='graph',figure={'data': data,'layout': {'margin': {'l': 30,'r': 0,'b': 30,'t': 30}}}),
            generateTable(systems_data)
                        ])
    if tab == "pipes":
        pipes_data = getPipesData()
        pipes_count = pipes_data.groupby("config.original.source.type")._id.count()
        data = [{'labels': list(pipes_count.index),'values': list(pipes_count),'type': 'pie'}]
        return html.Div([
            dcc.Graph(id='graph',figure={'data': data,'layout': {'margin': {'l': 30,'r': 0,'b': 30,'t': 30}}}),
            generateTable(pipes_data[pipes_data.columns.to_list()[0:6]])
            ])
    if tab == "entities":
        node_entities = getEntities("test-rest-brreg-naeringskode")
        df_tmp = node_entities.copy()
        map_tmp = list(map(lambda s:list(str.split(s,'.')),list(df_tmp.columns)))
        map_tmp = pd.DataFrame(map_tmp).fillna('')
        df_tmp.columns = pd.MultiIndex.from_frame(map_tmp)
        df_tmp_str_stat = df_tmp.applymap(cell_len).describe(include="all").T
        df_tmp_str_stat['type'] =[type(c).__name__ for c in df_tmp.iloc[0]]
        return html.Div([
            html.Iframe(srcDoc=pd.DataFrame(df_tmp_str_stat).to_html(),style={"width":"100%","height": "600"}, height=600)
                        ])


if __name__ == "__main__":
    serve(server,config={"debug": True})