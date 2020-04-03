import os
from datetime import datetime

import json

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
AUTH = f"Bearer: {JWT}"

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
        with requests.Session() as session:
            # session.auth = AUTH
            resp = session.get(URL, timeout=180)
            return Response(stream_as_json([resp.json()]), mimetype='application/json; charset=utf-8')
    except Timeout as e:
        logger.error(f"Timeout issue while fetching {e}")
    except ConnectionError as e:
        logger.error(f"ConnectionError issue while fetching {e}")
    except Exception as e:
        logger.error(f"Issue while fetching {e}")

if __name__ == "__main__":
    serve(server)