import json
from flask import Flask, jsonify, request
from flask_cors import CORS

from helper_function.hf_string import to_json_str
from helper_function.wrappers import api_status_wrapper

app = Flask(__name__)



def get_in_json_obj(req):
    data_bytes = req.get_data()
    data_str = data_bytes.decode()
    in_json_obj = json.loads(data_str)
    print(to_json_str(in_json_obj))
    return in_json_obj


@app.route('/api/health', methods=['GET', 'POST'])
@api_status_wrapper
def api_health():
    return {}


@app.route('/api/hello', methods=['GET', 'POST'])
@api_status_wrapper
def api_hello():
    in_json_obj = get_in_json_obj(request)
    username = in_json_obj['username']
    return f'Hello, {username}'
