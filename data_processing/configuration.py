'''
Пользовательская конфигурация.

Для корректных настроек под требования вашего кластера
нужно обратиться к официальной документации:
https://spark.apache.org/docs/latest/configuration.html

Запуск прямо из jupyter notebook - %run configuration.py
Или с указанием полного пути, например так:
%run ~/nfs/user/configuration.py

Данный файл унифицирован для дедупликации кода.
'''
from pyspark import SparkConf
from pyspark.sql import SparkSession

_conf = (
    SparkConf()
    .setAppName('data')
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

if __name__ == '__main__':
    spark
