import json


def pars_config(file: str) -> dict:
    """
    Считываем .json файл
    :param file: название считываемого файла
    :return: словарь полученный из файла
    """
    with open(file, 'r', encoding='utf-8') as fl:
        return json.load(fl)
