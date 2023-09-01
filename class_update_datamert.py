import psycopg2
from datetime import date
from sqlalchemy import create_engine
import pandas as pd
from pandas.core.frame import DataFrame
import os
import clickhouse_connect


class Update_table:
    """
    Класс для обновления региональных ветрин данных
    """
    def __init__(self, dct_cfg_pg: dict, dct_cfg_ch: dict):
        """
        :param dct_cfg_pg: словарь с параметрами подключения к базе данных Postgre
        :param dct_cfg_ch: словарь с параметрами подключения к базе данных Clickhouse
        """
        # Коннект для выгрузки данных из Postgre
        self.psycopg_connect = psycopg2.connect(user=dct_cfg_pg['USER'],
                                                password=dct_cfg_pg['PASSWORD'],
                                                host=dct_cfg_pg['HOST'],
                                                port=dct_cfg_pg['PORT'],
                                                database=dct_cfg_pg['DATABASE'])

        # Движок для загрузки данных в Postgre
        self.sqlalchemy_engine = create_engine('postgresql://{}:{}@{}:{}/{}'
                                               .format(dct_cfg_pg['USER'], dct_cfg_pg['PASSWORD'],
                                                       dct_cfg_pg['HOST'],
                                                       dct_cfg_pg['PORT'],
                                                       dct_cfg_pg['DATABASE']))

        # Клиент для инициализации работы с БД Clickhouse
        self.click_house_client = clickhouse_connect.get_client(host=dct_cfg_ch['HOST'],
                                                                port=dct_cfg_ch['PORT'],
                                                                username=dct_cfg_ch['USER'],
                                                                password=dct_cfg_ch['PASSWORD'],
                                                                database=dct_cfg_ch['DATABASE'])
        self.cursor = self.psycopg_connect.cursor()

    def __create_table_by_postgre(self, script: str) -> DataFrame:
        """
        :param script: текст sql запроса
        :return: датафрейм сформированный на базе Postgre
        """
        return pd.read_sql(script, con=self.psycopg_connect)

    @staticmethod
    def creation_beetween_period(x: int, y: str) -> str:
        """
        :param x: Год определенной записи
        :param y: Маркер, к какому типу относится запись Годовые/Сопоставимые
        :return: строка подходящая под паттерн period в БД
        """
        if y == 'Годовые':
            return str(x) + '-12-01'
        else:
            return date.today().strftime("%Y-%m-%d")

    def update_from_file(self, file_name: str, shema_name: str, table_name: str):
        """
        Функция обновления таблицы Рыба8 в Postgre
        :param shema_name: имя схемы данных Postgre
        :param file_name: имя файла из которого пойдет обновление
        :param table_name: имя таблицы
        :return:
        """
        trunc = f'TRUNCATE TABLE {shema_name}.{table_name}'
        vacuum = f'VACUUM (FULL, ANALYZE) {shema_name}.{table_name}'
        with self.cursor as cur:
            cur.execute(trunc)
            self.psycopg_connect.commit()
            self.psycopg_connect.set_session(autocommit=True)
            cur.execute(vacuum)
        df = pd.read_excel(file_name)
        df['period'] = df.apply(lambda x: self.creation_beetween_period(x.year, x.between_period), axis=1)
        df.to_sql(table_name, con=self.sqlalchemy_engine, schema=shema_name, if_exists='append', index=False)
        os.remove(file_name)
        print('Рыба8 обновлена')

    def update_data_postgre_to_clichouse(self, script: str, table_name: str):
        """
        :param script: sql скрипт с собираемой витирной данных
        :param table_name: имя обновляемой таблицы в ClickHouse
        :return:
        """
        df = self.__create_table_by_postgre(script)
        df['date_update'] = df.period.max()
        self.click_house_client.command(f'TRUNCATE TABLE IF EXISTS {table_name}')
        self.click_house_client.insert_df(table=table_name, df=df)

    def update_table_by_insert(self, script: str, name_bd: str, table_name: str):
        """
        :param script: sql скрипт собирающий таблицу на базе ClicHouse
        :param name_bd: имя базы данных
        :param table_name: имя обновляемой таблицы
        :return:
        """
        self.click_house_client.command(f'TRUNCATE TABLE IF EXISTS {table_name}')
        self.click_house_client.command(f"""INSERT INTO {name_bd}.{table_name}
                                            {script}""")

    def update_ref_table(self, script: str, table_name: str):
        """
        :param script: Скрипт перекачки данных с Postgre на ClickHouse
        :param table_name: имя обновляемой таблицы
        :return:
        """
        df_ref = self.__create_table_by_postgre(script)

        self.click_house_client.command(f'TRUNCATE TABLE IF EXISTS {table_name}')
        self.click_house_client.insert_df(table=table_name, df=df_ref)

    def all_close(self):
        self.psycopg_connect.close()
        self.click_house_client.close()









