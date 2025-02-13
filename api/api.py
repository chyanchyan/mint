from collections import defaultdict

from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import urllib.parse

import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from mint.helper_function.wrappers import api_status_wrapper
from mint.api.api_functions import *
from mint.api.api_curd import *

app = Flask(__name__)
CORS(app=app)


def get_in_json_obj(req):
    data_bytes = req.get_data()
    data_str = data_bytes.decode()
    in_json_obj = json.loads(data_str)
    jo = defaultdict(lambda: None, in_json_obj)
    return jo


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


@app.route('/api/getRightAngleTrees/', methods=['GET', 'POST'])
@api_status_wrapper
def api_get_get_right_angle_trees():
    jo = get_in_json_obj(req=request)
    res = get_right_angle_trees(jo=jo)
    return res


@app.route('/api/getSelectOptions', methods=['GET', 'POST'])
@api_status_wrapper
def api_get_select_options():
    jo = get_in_json_obj(req=request)
    con = get_con('data')
    res = get_select_options(con, **jo)
    con.close()
    return res


@app.route('/api/checkUnique', methods=['GET', 'POST'])
@api_status_wrapper
def api_check_unique():
    jo = get_in_json_obj(req=request)
    res = check_unique(jo)
    return res

@app.route('/api/checkUpdate', methods=['GET', 'POST'])
@api_status_wrapper
def api_check_update():
    jo = get_in_json_obj(req=request)
    res = check_update(jo)
    return res


@app.route('/api/stash', methods=['GET', 'POST'])
@api_status_wrapper
def api_stash():
    jo = get_in_json_obj(req=request)
    res = stash(jo)
    return res


@app.route('/api/getStashList', methods=['GET', 'POST'])
@api_status_wrapper
def api_get_stash_list():
    con = get_con('data')
    res = get_stash_list(con=con)
    con.close()
    return res


@app.route('/api/genBookingXlSheet', methods=['GET', 'POST'])
@api_status_wrapper
def api_gen_booking_xl_sheet():
    jo = get_in_json_obj(req=request)
    res = gen_booking_xl_sheet_file(
        jo=jo
    )
    return res


@app.route('/api/createTree', methods=['GET', 'POST'])
@api_status_wrapper
def api_create_tree():
    jo = get_in_json_obj(req=request)
    create_tree(jo)
    return


@app.route('/api/deleteTree', methods=['GET', 'POST'])
@api_status_wrapper
def api_delete_tree():
    jo = get_in_json_obj(req=request)
    res = delete_tree(jo=jo)
    return res


@app.route('/api/updateTree', methods=['GET', 'POST'])
@api_status_wrapper
def api_update_tree():
    jo = get_in_json_obj(req=request)
    res = update_tree(jo)
    return res


@app.route('/api/getSubmitPreviewTables', methods=['GET', 'POST'])
@api_status_wrapper
def api_get_submit_preview_tables():
    jo = get_in_json_obj(req=request)
    res = get_submit_preview_tables(jo)
    return res


@app.route('/api/exportTableToExcel', methods=['GET', 'POST'])
@api_status_wrapper
def api_export_table_to_excel():
    jo = get_in_json_obj(req=request)
    res = export_table_to_excel(jo=jo)
    return res


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8083, debug=True)


