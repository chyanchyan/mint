import json
import os
from flask import Flask, jsonify, request, send_file
from flask_cors import CORS

if 'mint' in __name__.split('.'):
    from .helper_function.hf_string import to_json_str
    from .helper_function.wrappers import api_status_wrapper, sql_retry_wrapper
else:
    from helper_function.hf_string import to_json_str
    from helper_function.wrappers import api_status_wrapper, sql_retry_wrapper

app = Flask(__name__)
CORS(app=app, supports_credentials=True)


def get_in_json_obj(req):
    data_bytes = req.get_data()
    data_str = data_bytes.decode()
    in_json_obj = json.loads(data_str)
    return in_json_obj


@app.route('/api/health', methods=['GET', 'POST'])
@api_status_wrapper
def api_health():
    return {}


@app.route('/api/fileDownload/<file_path>', methods=['get', 'post'])
def api_file_download(file_path):
    file_path_list = file_path.split('>')
    file_path = f'{os.path.sep}'.join(file_path_list)
    print(file_path)
    return send_file(file_path, as_attachment=True)


@app.route('/api/hello', methods=['GET', 'POST'])
@api_status_wrapper
def api_hello():
    in_json_obj = get_in_json_obj(request)
    username = in_json_obj['username']
    return f'Hello, {username}'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8083, debug=True)


