import json
import os
from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import urllib.parse
from werkzeug.utils import secure_filename

if 'mint' in __name__.split('.'):
    from .helper_function.hf_string import to_json_str
    from .helper_function.wrappers import api_status_wrapper, sql_retry_wrapper
    from .sys_init import *
else:
    from helper_function.hf_string import to_json_str
    from helper_function.wrappers import api_status_wrapper, sql_retry_wrapper
    from sys_init import *


app = Flask(__name__)
CORS(app=app)


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
    file_name = file_path_list[-1]
    file_path = f'{os.path.sep}'.join(file_path_list)

    encoded_file_name = urllib.parse.quote(file_name)
    print(file_path)
    res = send_file(file_path, as_attachment=True)
    res.headers['Content-Disposition'] = 'attachment; filename*=UTF-8\'\'{}'.format(encoded_file_name)
    return res


@app.route('/api/fileUpload', methods=['get', 'post'])
@api_status_wrapper
def api_file_upload():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file:
        filename = file.filename
        # filename = secure_filename(file.filename)
        filepath = os.path.join(PATH_UPLOAD, filename)
        file.save(filepath)
        print(f'file saved: {filepath}')
        return {'fileName': filename, 'filePath': filepath}
    else:
        return {'error': 'File type not allowed'}


@app.route('/api/hello', methods=['GET', 'POST'])
@api_status_wrapper
def api_hello():
    in_json_obj = get_in_json_obj(request)
    username = in_json_obj['username']
    return f'Hello, {username}'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8083, debug=True)


