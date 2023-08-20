import time
from tqdm import tqdm
import traceback


def timecost(running_time=1):
    def wrapper(func):
        def _(*args, **kwargs):
            total_time_cost = 0
            res = ''
            for _ in tqdm(range(running_time)):
                tic = time.time()
                res = func(*args, **kwargs)
                toc = time.time()
                total_time_cost += toc - tic

            print(f'\nrunning "{func.__name__}" {running_time} times, avg time cost: {total_time_cost / running_time}')
            return res
        return _
    return wrapper


def print_output(func):
    def _(*args, **kwargs):
        res = func(*args, **kwargs)
        print(res)
    return _


def get_func_params(func, args, kwargs):
    params = list(func.__code__.co_varnames[:func.__code__.co_argcount])
    defaults = func.__defaults__ or ()
    default_parameters = params[-len(defaults):]
    default_values = dict(zip(default_parameters, defaults))
    params = dict(zip(params, args))
    params.update(default_values)
    params.update(kwargs)
    return params


def check_param_valid_range(params_to_check=(), valid_ranges=(())):
    def wrapper(func):
        def _(*args, **kwargs):
            params = get_func_params(func, args, kwargs)
            for i, param_name in enumerate(params_to_check):
                try:
                    assert params[param_name] in valid_ranges[i]
                except AssertionError:
                    value_str = '\n'.join([item[0] + " =\t" + item[1].__repr__() for item in params.items()])
                    print(f'unsupported method: \'{params[param_name]}\' besides {valid_ranges[i]}')
                    print(f'when doing: {func.__name__} \n'
                          f'with args: \n'
                          f'{value_str}')
                    return ''
            return func(*args, **kwargs)

        _.__name__ = func.__name__

        return _
    return wrapper


def api_status_wrapper(func):
    def wrapper(*args, **kwargs):
        status = 0
        status_info = 'success'

        try:
            data = func(*args, **kwargs)
        except Exception as e:
            print(traceback.format_exc())
            print(repr(e))
            data = {}
            status = 1
            status_info = traceback.format_exc().split('\n') + [repr(e)]

        api = dict()
        api['data'] = data
        api['status'] = status
        api['statusInfo'] = status_info
        return api

    wrapper.__name__ = func.__name__

    return wrapper


def sub_wrapper(sub):
    def wrapper(*args, **kwargs):
        status = 0
        try:
            print('running %s' % sub.__name__)
            sub(*args, **kwargs)
            print('%s done' % sub.__name__)
        except Exception as e:
            print(traceback.format_exc())
            print(repr(e))
            status = 1

        return status

    wrapper.__name__ = sub.__name__

    return wrapper


def confirm_wrapper(sub):
    def wrapper(*args, **kwargs):
        ipt = input('typing "yes" to run %s' % sub.__name__)
        if ipt == 'yes':
            sub(*args, **kwargs)
        else:
            print('aborting.')

    wrapper.__name__ = sub.__name__

    return wrapper


def timer_wrapper(sub):
    def wrapper(*args, **kwargs):
        tic = time.time()
        res = sub(*args, **kwargs)
        toc = time.time()

        time_cost_str = '{:.4f}s'.format((toc - tic))
        print('time cost: [%s] %s' % (time_cost_str, sub.__name__))
        return res

    wrapper.__name__ = sub.__name__

    return wrapper


class PropertyIndexer:
    def __init__(self, instance, property_name):
        self.instance = instance
        self.property_name = property_name

    def __getitem__(self, args):
        if isinstance(args, (list, tuple)):
            return eval(f'self.instance.{self.property_name}(*args)')
        elif isinstance(args, str):
            return eval(f'self.instance.{self.property_name}(args)')
