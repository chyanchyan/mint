import os
from datetime import datetime as dt
from shutil import copy
from helper_function.func import sub_wrapper


@sub_wrapper
def mkdir(path):
    path_copy = str(path)
    while not os.path.exists(path_copy):
        try:
            os.mkdir(path_copy)
        except FileNotFoundError:
            path_copy = os.path.dirname(path_copy)
            mkdir(path_copy)


@sub_wrapper
def snapshot(src_path, dst_folder, auto_timestamp=True, comments=''):
    mkdir(dst_folder)
    if auto_timestamp:
        time_str = dt.now().strftime('%Y%m%d_%H%M%S_%f')
    else:
        time_str = ''

    filename_base, ext = os.path.basename(src_path).split('.')

    dst_file_name_base = '_'.join([item for item in [filename_base, time_str, comments] if len(item) > 0])
    dst_file_name = f'{dst_file_name_base}.{ext}'
    dst_path = os.path.join(dst_folder, dst_file_name)

    copy(src=src_path, dst=dst_path)
