from datetime import datetime
from functools import wraps


def func_decorator(func):
    """
    Функция декоратор высчитывающая время выполнения переданной функции в минутах
    :param func: декорируемая функция
    :return: вложенная функция
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        date_start = datetime.now()
        res = func(*args, **kwargs)
        print(round((datetime.now() - date_start).total_seconds() / 60, 2), f'Витрины обновлены')
        return res

    return wrapper
