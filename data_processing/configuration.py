# Пользовательская конфигурация.
#
# Данные настройки для инициализации SparkSession относятся к кластеру,
# имеющему следующие характеристики (суммарно):
# Кол-во нод - 6, 96 CPU, 768GB RAM, 5.5TB HDD
#
# Для корректных настроек под требования вашего кластера
# нужно обратиться к официальной документации:
# https://spark.apache.org/docs/latest/configuration.html
#
# В данной конфигурации реализованы несколько пользовательских функций
# для быстрой аналитики больших данных, которые чаще всего применялись.
#
# Запуск прямо из jupyter notebook - %run configuration.py
# Или с указанием полного пути, например так:
# %run /home/usr/notebooks/data_processing/configuration.py
import os
import sys
import subprocess

_PYTHON_PATH = sys.executable

os.environ['SPARK_MAJOR_VERSION'] = '3'
os.environ['SPARK_HOME'] = '/usr/sdp/current/spark3-client/'
os.environ['PYSPARK_DRIVER_PYTHON'] = _PYTHON_PATH
os.environ['PYSPARK_DRIVER'] = _PYTHON_PATH
os.environ['PYSPARK_PYTHON'] = _PYTHON_PATH
os.environ['LD_LIBRARY_PATH'] = '/opt/python/virtualenv/jupyter/lib'
sys.path.insert(0, '/usr/sdp/current/spark3-client/python')
sys.path.insert(
    0,
    '/usr/sdp/current/spark3-client/python/lib/py4j-0.10.9.3-src.zip'
)

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import pyspark.sql.types as T

import re
import csv
import numpy as np
import pandas as pd
import datetime as dt

_conf = (
    SparkConf()
    .setAppName('data_research')
    .setMaster('yarn')
    .set('spark.port.maxRetries', '150')
    .set('spark.executor.cores', '2')
    .set('spark.executor.memory', '6g')
    .set('spark.executor.memoryOfHead', '1g')
    .set('spark.driver.memory', '6g')
    .set('spark.driver.maxResultSize', '4g')
    .set('spark.shuffle.service.enabled', 'true')
    .set(
        'spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive',
        'true'
    )
    .set('spark.dynamicAllocation.enabled', 'true')
    .set('spark.dynamicAllocation.executorIdleTimeout', '120s')
    .set('spark.dynamicAllocation.cachedExecutorIdleTimeout', '600s')
    .set('spark.dynamicAllocation.initialExecutors', '3')
    .set('spark.dynamicAllocation.maxExecutors', '12')
)

spark = SparkSession.builder.config(conf=_conf).getOrCreate()


class Analitics:
    """
    Класс самописных вспомогательных функций для быстрой аналитики.
    """
    def getDatabases() -> list:
        """
        Получить список доступных БД из Hive.
        Проверить наличие переданного наименования БД в списке всех БД.
        listDatabases() возвращает не только наименования, но ещё
        и дополнительную информацию (описания, ссылки), поэтому
        забираем только названия через цикл.
        """
        information_of_databases: list = spark.catalog.listDataBases()
        list_of_databases_name: list = []
        for database_name, description, link in information_of_databases:
            list_of_databases_name.append(database_name)
        return list_of_databases_name
    

    def getTables(database: str) -> list:
        """
        Получить список таблиц по названию БД, 
        проверив перед этим корректность наименования БД 
        через наличие/отсутствие её в списке - getDatabases().
        """
        list_of_databases_name: list = Analitics.getDatabases(database)
        if database not in list_of_databases_name:
            print(f"БД: {database} - отсутствует")
        else:
            list_of_tables_name: list = []
            information_of_tables: list = spark.catalog.listTables(database)
            for table in information_of_tables:
                list_of_tables_name.append(table[0])
            return list_of_tables_name


    def getPath(database: str, table: str) -> list:
        """
        Найти путь до директории с файлами таблиц.
        Получить список путей до всех parquet-файлов нужной таблицы в HDFS.
        В директории таблиц parquet-файлы организованы по-разному:
        1) Если кол-во файлов больше >= 3, то забираем предпоследний файл,
        так как в последнем может быть не достаточно данных для анализа.
        2) Если кол-во файлов меньше, то забираем последний/единственный,
        так как первый может быть схемой, а не набором данных.
        3) Если таблица пустая, то возвращаем пустой список.
        """
        path_to_tables: any = (spark.sql(
            f"describe formatted {database}.{table};")
            .filter(F.col('col_name') == 'Location')
            .select('data_type').collect()[0])
        path: list[bytes] = subprocess.run(
            ['hdfs', 'dfs', '-ls', f"{path_to_tables[0]}"],
            stdout=subprocess.PIPE).stdout.splitlines()
        if path == []:
            return path
        else:
            index: int = -2 if len(path) >= 3 else -1
            parquet: str = path[index].decode()
            parquet_index: int = -1
            path_string: str = (parquet.split(sep=' '))[parquet_index]
            return path_string


    def getExcel(writer, # type ContextManager
                 worksheet_name: str, dataframe: DataFrame) -> None:
        """
        Записать все результаты обработки данных в excel-файл.
        По каждой таблице на отдельный лист.
        Шаблон применения функции будет выглядеть как-то так:
        with pd.ExcelWriter('имя_файла', 'движок') as writer:
            for 'таблица' in 'список_таблиц':
                getExcel(аргументы)
        """
        sheet_length: int = 31 # длина имени листа не более 31
        dataframe.to_excel(writer, 
                           sheet_name=f"{worksheet_name[sheet_length]}",
                           index=False, encoding='utf-8')


    def getSample(database: str) -> None:
        """
        С помощью контекстного менеджера открываем запись в excel-файл.
        Используем пользовательские функции getDatabases(), getTables(),
        getPath(), getExcel() для проверки наличия базы данных,
        получения списка таблиц, поиска parquet-файлов и записи результатов.
        В текущей функции производим обработку данных в контексте
        изменения типа столбцов с timestamp на string для избежания
        конфликта между PySpark и Pandas/NumPy.
        """
        with pd.ExcelWriter(
            f"{database}.xlsx", engine='xlsxwriter') as writer:
            for table in Analitics.getTables(database):
                path: str = Analitics.getPath(database, table)
                if path == []:
                    pass
                else:
                    dataframe: DataFrame = spark.read.parquet(path)
                    string_columns: list = (
                        [F.col(column).cast(T.StringType())
                         for column in dataframe.columns])
                    number_of_rows: int = 100 # Можно увеличить размер сэмпла
                    changed_df: pd.DataFrame = (dataframe.select(
                        *string_columns).limit(number_of_rows)).toPandas()
                    path_of_the_name: list = table.split(sep='_')
                    sheet_name: str = path_of_the_name[-1]
                    Analitics.getExcel(writer, sheet_name, changed_df)
    

    def getSchema(schema_dict: dict, dataframe: DataFrame) -> DataFrame:
        """
        Переопределяем схему для PySpark DataFrame с помощью работы через RDD.
        Дополняет метаданные (комментариями) при чтении/просмотре таблицы.
        Можно дополнить изменениями на разные типы данных внутри цикла.
        """
        schema_with_metadata: list = []
        for name, comment in schema_dict.items():
            point: any = T.StructField(
                f"{name}", T.StringType(), True, {'comment': f'{comment}'})
            schema_with_metadata.append(point)
        new_dataframe: DataFrame = spark.createDataFrame(
            dataframe.rdd, T.StructType(schema_with_metadata))
        return new_dataframe


    def getAmount(database: str) -> pd.Series:
        """
        Подсчёт кол-ва записей каждой таблицы определённой БД.
        Поиск в объекте вычисления числового значения с помощью
        регулярного выражения, перевод из строковых значений
        в целочисленный тип внутри генератора и запись в Pandas Series.
        """
        database_composition: list = Analitics.getTables(database)
        couples: dict = {}
        for table in database_composition:
            counter: any = spark.sql(
                f"select (*) as {table} from {database}.{table};"
            ).collect()
            pattern: str = r'(\d+)'
            amount_search: any = re.findall(pattern, str(counter))
            [couples.update({table: int(count)}) for count in amount_search]
        series: pd.Series = pd.Series(
            data=couples.values(), index=couples.keys())
        print(series)
    

    def getEntry(database: str, target: str) -> str:
        """
        Поиск необходимого значения во всех столбцах всех таблиц одной БД.
        Проверка на кол-во вхождений и вывод статистики.
        Все промежуточные csv-файлы удаляются автоматически, сразу после
        анализа фрейма.
        """
        for table in Analitics.getTables(database):
            path: list[str] = Analitics.getPath(database, table)
            if path == []:
                pass
            else:
                max_lines: int = 100000 # лимит зависит от размера таблиц
                pandas_df: pd.DataFrame = spark.read.parquet(
                    path).limit(max_lines).toPandas()
                file_name: str = f"{table}.csv"
                pandas_df.to_csv(file_name)
                with open(file_name, 'rt') as file:
                    reader: any = csv.reader(file, delimiter=',')
                    counter: list = []
                    for row in reader:
                        if target in row:
                            counter.append(row)
                    print(f"{table}: {len(counter)}")
                    if os.path.exists(file_name):
                        os.remove(file_name)


    def getFunctions() -> list[any]:
        """
        Посмотреть все имена и описания пользовательских функций.
        """
        list_of_functions: list = [Analitics.getDatabases, Analitics.getTables,
                                  Analitics.getPath, Analitics.getExcel,
                                  Analitics.getSample, Analitics.getAmount,
                                  Analitics.getSchema, Analitics.getEntry]
        return [help(my_function) for my_function in list_of_functions]


if __name__ == '__main__':
    spark # Инициализация SparkSession
