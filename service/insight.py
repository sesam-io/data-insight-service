import os
from datetime import datetime
import json
import pandas as pd

from flask import Flask, request, jsonify, Response, abort

import requests
from requests.exceptions import Timeout

from sesamutils import sesam_logger, VariablesConfig
from sesamutils.flask import serve


server = Flask(__name__)
logger = sesam_logger('insight', app=server, timestamp=True)

# Default values can be given to optional environment variables by the use of tuples
required_env_vars = ["JWT", "URL"]
optional_env_vars = [("LOG_LEVEL", "INFO")] 
config = VariablesConfig(required_env_vars, optional_env_vars=optional_env_vars)
    
if not config.validate():
    logger.error("Environment variables do not validate. Exiting system.")
    os.sys.exit(1)

JWT = config.JWT
URL = config.URL
HEADER = {'Authorization': f'Bearer {JWT}'}

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

@server.route("/",methods=["GET"]) 
def get_url():
    try:
        if request.args.get('since') is None:
            logger.debug(f"since value not set")
        else:
            unix_time_update_date = request.args.get('since')
            logger.debug(f"since value sent from sesam: {unix_time_update_date}")
        with requests.Session() as session: # For streaming with generator
            session.headers.update(HEADER)
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

@server.route("/stats",methods=["POST"]) 
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

@server.route("/flaten",methods=["POST"]) 
def post_flaten():
    payload = request.json
    data = pd.json_normalize(payload,sep='~')
    return Response(data.to_json(orient="records"), mimetype='application/json; charset=utf-8')


if __name__ == "__main__":
    serve(server)