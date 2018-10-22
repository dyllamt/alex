from subprocess import Popen, DEVNULL

from pandas import DataFrame, concat
from pandas.io.json import json_normalize
from pymongo import MongoClient

import numpy as np

'''
A workspace contains a variety of data sources. You can transfer structured
data between all workspace sources through instance attributes and methods.
'''


def local_connection(func):
    '''
    spawn a mongod thread and enable access to storage around an operation.
    errors during execution are handled after terminating the mongod process.

    Args:
        func (function) a function that requires access to a local mongodb
    '''

    def wrapper(self, *args, **kwargs):

        # spawn a mongod process and set up a connection
        mongod = Popen(['mongod', '--dbpath', self.path], stdout=DEVNULL)
        self.connection = MongoClient()[self.database][self.collection]
        print('connected to {}.{}'.format(self.database, self.collection))

        # execute operation
        error = None
        try:  # attempt to execute operation
            result = func(self, *args, **kwargs)
        except Exception as e:  # put off raising errors till later
            error = e

        # break connection and terminate mongod process
        self.connection = None
        mongod.terminate()
        mongod.wait()
        print('disconnected')

        if error:  # errors in execution are raised after disconnection
            raise error

        return result

    return wrapper


class MongoFrame(object):
    '''
    abstraction for structured data in pymongo Collections (storage) and pandas
    DataFrames (memory). methods support data transfers between storage and
    memory. storage and memory are treated as instance attributes. the index
    attribute of DataFrames are not stored or preserved in compression

    Attributes:
        path (str) path to a mongodb directory
        database (str) name of a pymongo database
        collection (str) name of a pymongo collection
        connection (Collection|None) statefull connection to storage
        memory (DataFrame|None) pandas dataframe for temporary storage
    '''

    def __init__(self, path, database, collection, memory=None):
        '''
        Args:
            path (str) path to a mongodb directory
            database (str) name of a pymongo database
            collection (str) name of a pymongo collection
            memory (DataFrame|None) pandas dataframe
        '''
        self.path = path
        self.database = database
        self.collection = collection
        self.connection = None
        self.memory = memory

    @local_connection
    def save_to_storage(self, identifier, upsert=True):
        '''
        save data in memory (DataFrame) to storage (Collection)

        Args:
            identifier (str|None) document field (column) of unique identifier.
                if None then unique insertion is not enforced
            upsert (bool) insert missing documents in unique insertion mode
        '''
        if identifier:  # unique insertion mode
            for row in self.memory.to_dict(orient='records'):
                self.connection.update_one(
                    filter={identifier: row[identifier]},
                    update={'$set': row},
                    upsert=upsert)
        else:  # documents are non-unique
            self.connection.insert_many(
                self.memory.to_dict(orient='records'))

    @local_connection
    def load_to_memory(self, find={}):
        '''
        load data from storage (Collection) to memory (DataFrame)

        args:
            find (dict) optional arguments to pass to pymongo.find
        '''
        self.memory = DataFrame.from_records(
            list(self.connection.find(**find)))

    @local_connection
    def delete_storage(self, filter={}, clear_collection=False):
        '''
        delete collection documents with a pymongo query operator

        Args:
            filter (son) pymongo query operator passed to delete_many()
            clear_collection (bool) clear storage entirely
        '''
        if clear_collection:  # remove all documents
            self.connection.delete_many({})
        elif filter:  # remove documents matching kwargs
            self.connection.delete_many(filter)
        else:  # make sure collection purge is intended
            raise Exception('Do you mean to delete everything in {}.{}? If so,'
                            'then flag clear_collection as True.'.format(
                                self.database, self.collection))

    def memory_compression(self, column, decompress=False):
        '''
        compress all columns into one parent column or expand a single column

        Args:
            column (str) data field name to expand or compress into
            decompress (bool) choose between column compression/decompression
        '''
        if decompress:
            decompressed = json_normalize(
                self.memory[[column]].to_dict(orient='records'))
            self.memory.drop(columns=column, inplace=True)
            self.memory = concat([self.memory, decompressed], axis=1)
        else:
            compressed = self.memory.to_dict(orient='records')
            self.memory = DataFrame(data=np.array(compressed),
                                    columns=[column])
