'''
В данном модуле реализованы несколько пользовательских
классов и функций для быстрой аналитики больших данных.
'''
import subprocess
from configuration import spark
from pyspark.sql import DataFrame
import pyspark.sql.functions as F


class Data:

    '''
    Класс, расширяющий базовые возможности
    фреймворка pyspark, для быстрой аналитики.
    '''

    _first: int = 0
    _last: int = -1
    for_console: int = 999

    @staticmethod
    def writeExcel():
        '''
        Удобная объединённая запись данных в excel-файл.
        -> df.toPandas().to_excel(*arg)

        А также добавление необходимых параметров для варианта
        использования цикличной записи через writer.
        '''

        pass

    @staticmethod
    def viewEntries():
        '''Дополненный метод show() для просмотра.'''

        pass


class Subscriptions(Data):

    '''
    Класс для просмотра основной информации
    по конкретным источникам данных.
    '''

    @staticmethod
    def showDatabases():
        '''Показать интересующие источники данных.'''

        pass

    @staticmethod
    def showTables():
        '''Показать таблицы источника данных.'''

        pass

    @staticmethod
    def showAttributes():
        '''Показать атрибуты таблицы с описанием.'''

        pass

    @staticmethod
    def showPartitions():
        '''Показать партиции/разделы таблицы.'''

        pass


class Parquet(Data):

    '''Класс работы с parquet-файлами.'''

    @staticmethod
    def getPaths():
        '''Получить все пути к партициям/разделам таблицы.'''

        pass

    @staticmethod
    def getOne():
        '''
        Получить один раздел/parquet-файл.

        Для корректной работы забираем единственный/последний
        файл, так как первый может быть служебным маркером "SUCCESS",
        а не набором данных.
        '''

        pass

    @staticmethod
    def getFrame():
        '''
        Получить фрейм данных из одного parquet-файла.

        Ускорение обращения к набору данных за счёт чтения одного файла,
        вместо всей таблицы:
        -> spark.table('database.table') [-]
        -> spark.read.parquet('file') [+]

        Полезно, если нужно быстро проанализировать данные
        или собрать прототип витрины.
        '''

        pass
