# Пользовательская конфигурация.
#
# Данные настройки для инициализации SparkSession относятся к кластеру,
# имеющему следующие характеристики (суммарно):
# Кол-во нод - 6
# 96 CPU
# 768GB RAM
# 5.5TB HDD
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
import numpy as np
import pandas as pd
import datetime as dt
import re

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

spark = (
    SparkSession
    .builder
    .enableHiveSupport()
    .config(conf=_conf)
    .getOrCreate()
)


def getDatabases(database: str) -> list:
    """
    Получить список доступных БД из Hive.
    Проверить наличие переданного наименования БД в списоке всех БД.
    """
    information_of_databases: list = spark.catalog.listDataBases()
    list_of_databases_name: list = []
    for database_name, description, link in information_of_databases:
        list_of_databases_name.append(database_name)
    if database not in list_of_databases_name:
        print(f"БД: {database} - отсутствует.")
    else:
        return list_of_databases_name
    

def getTables(database: str) -> list:
    """
    Получить список таблиц по названию БД.
    """
    getDatabases(database)
    list_of_tables_name: list = spark.catalog.listTables(database)
    return list_of_tables_name


def getCounter(database: str) -> DataFrame:
    """
    Подсчёт кол-ва записей каждой таблицы определённой БД.
    """
    getDatabases(database)
    database_composition: list = getTables(database)
    for table in database_composition:
        globals()[table[0]] = table[0].split(sep='_')
        counter: any = spark.sql(
            f"""
            select (*) as {globals()[table[0]][-1]} 
            from {database}.{table}
            """
        ).collect()
        print(counter)


def getPath(database: str, table: str) -> list:
    """
    Найти путь до директории с файлами таблиц.
    Получить список путей до всех parquet-файлов нужной таблицы в HDFS.
    """
    path_to_database: any = (
        spark.sql(f"describe formatted {database}.{table}")
        .filter(F.col('col_name') == 'Location')
        .select('data_type').collect()[0]
    )
    list_of_paths: list[bytes] = subprocess.run(
        ['hdfs', 'dfs', '-ls',
         f"{path_to_database[0]}"],
         stdout=subprocess.PIPE).stdout.splitlines()
    return list_of_paths


def getExcel(writer: object, table: str, dataframe: DataFrame) -> None:
    """
    Записать все результаты обработки данных в excel-файл.
    По каждой таблице на отдельный лист.
    """
    path_of_the_name: list = table[0].split(sep='_')
    worksheet_name: str = path_of_the_name[-1]
    sheet_length: int = 31
    dataframe.toPandas().to_excel(
        writer,
        sheet_name=f"{worksheet_name[sheet_length]}",
        index=False,
        encoding='utf-8'
    )


def createSchema(schema_dict: dict, dataframe: DataFrame) -> DataFrame:
    """
    Определяем новую схему для DataFrame.
    Дополняет метаданные (комментариями) к таблице в Hive.
    """
    schema_with_metadata = []
    for name, comment in schema_dict.items():
        point = T.StructField(f"{name}", T.StringType(), True,
                              {'comment': f'{comment}'})
        schema_with_metadata.append(point)
    new_dataframe = spark.createDataFrame(dataframe.rdd,
                                          T.StructType(schema_with_metadata))
    return new_dataframe


def getLimited(database: str) -> None:
    """
    Выгружаем по 100 строк из каждой таблицы БД
    и записываем в excel-файл для анализа.

    Шаги:

    - Проверяем наличие БД из списка в Hive
    - Создаём экземпляр записи в Excel
    - Находим путь до БД в HDFS
    - По этому пути забираем последний parquet-файл конкретной таблицы из БД
    - Формируем DataFrame на основе чтения parquet-файла
    - Формируем новый DataFrame со столбцами типа string
    - Записываем по 100 строк на каждый лист в excel-файл

    Избежание ошибок и длительной обработки:

    --- Проверяем наличие БД в списке доступных
    --- Для быстрого доступа читаем один parquet-файл вместо всей таблицы
    --- Избегаем конфликта меток timestamp между PySpark и Pandas/NumPy
        через присвоение всем столбцам фрейма типа данных - string
    --- Устанавливаем константу длины наименования листа Excel для
        выполнения условия: длина <= 31
    """
    getDatabases(database)
    with pd.ExcelWriter(f"{database}.xlsx", engine='xlsxwriter') as writer:
        for table in getTables(database):
            path: list[bytes] = getPath(database, table)
            index_file: int = index_path: int = -1
            parquet_file: any = path[index_file].decode()
            path_string: str = parquet_file.split(sep=' ')[index_path]
            dataframe: DataFrame = spark.read.parquet(path_string)
            string_columns: list = ([F.col(column).cast(T.StringType())
                                     for column in dataframe.columns])
            number_of_rows: int = 100
            changed_dataframe: DataFrame = (
                dataframe.select(*string_columns).limit(number_of_rows)
            )
            table_entry: None = getExcel(writer, table, changed_dataframe)


if __name__ == '__main__':
    spark # Инициализация SparkSession
