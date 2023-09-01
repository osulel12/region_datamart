from class_update_datamert import Update_table
import os
from dotenv import load_dotenv
import warnings
from decorator_func import func_decorator
from config_parser import pars_config
from sql_scripts import sql_update_main_table, sql_script_region_coords, \
    sql_script_macro_region, sql_update_chart_product, sql_update_speed, sql_update_need

warnings.simplefilter('ignore')


@func_decorator
def main():
    # Подгружаем наши переменные окружения
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)

    # Подгружаем словарь с названиями таблиц и БД
    dict_name_table = pars_config('config_js.json')

    # Формируем конфиг словари для подключения к базам данных
    dct_pg = {'USER': os.getenv('USER_NAME_PG'), 'PASSWORD': os.getenv('PASSWORD_PG'),
              'HOST': os.getenv('HOST_PG'), 'PORT': os.getenv('PORT_PG'), 'DATABASE': os.getenv('DATABASE_PG')}
    dct_ch = {'USER': os.getenv('USER_NAME_CLICKHOUSE'), 'PASSWORD': os.getenv('PASSWORD_CLICKHOUSE'),
              'HOST': os.getenv('HOST_CLICKHOUSE'), 'PORT': os.getenv('PORT_CLICKHOUSE'),
              'DATABASE': os.getenv('DATABASE_CLICKHOUSE')}

    # Создаем экземпляр класса
    update_datamart = Update_table(dct_pg, dct_ch)

    # Если присутствует нужный файл, то обновляем справочники
    if os.path.isfile(dict_name_table['file_update_ref']):
        for script, table in zip([sql_script_region_coords, sql_script_macro_region],
                                 [dict_name_table['ref_coord_table'], dict_name_table['ref_macro_region']]):
            update_datamart.update_ref_table(script, table)

    # Если присутствует файл обновления таблицы, то обновляем ее
    if os.path.isfile(dict_name_table['file_name']):
        update_datamart.update_from_file(dict_name_table['file_name'], dict_name_table['shema_pg'], dict_name_table['table_fh'])

    # Обновляем основную таблицу
    update_datamart.update_data_postgre_to_clichouse(sql_update_main_table, dict_name_table['table_main'])

    # Обновляем дочерние таблицы
    # Так как они обновляются на основе главной, делаем это действие в конце
    for script, table in zip([sql_update_chart_product, sql_update_speed, sql_update_need],
                             [dict_name_table['table_chart'], dict_name_table['speed_table'], dict_name_table['need_table']]):
        update_datamart.update_table_by_insert(script, dict_name_table['bd_click'], table)

    # Закрываем подключения
    update_datamart.all_close()


if __name__ == "__main__":
    main()

