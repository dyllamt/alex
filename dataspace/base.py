from pandas import DataFrame, concat
from pandas.io.json import json_normalize

import numpy as np

'''
this module defines key objects for data exploration:

1. Workspace - an object for handeling structured data. a workspace always has
    a memory attribute for storing data during run-time, but may also have
    connections to other data sources such as APIs or local databases.

2. Pipe - an object for passing data between two Workspace instances. data is
    transfered using the memory attributes of both Workspaces.
'''


class Workspace(object):
    '''
    workspaces encompass various structured data sources. examples of sources
    include api retrievers and local databases (mongodb). all workspaces
    contain a memory attribute for temporary data storage in pandas DataFrames

    Attributes:
        memory (DataFrame|None) pandas dataframe for temporary storage
        connection (object) a statefull connection to the data source
    '''
    def __init__(self):
        self.memory = None
        self.connection = None

    def to_storage(self):
        '''
        transfer data from memory to source
        '''

        raise NotImplementedError("to_storage() is not defined!")

    def from_storage(self):
        '''
        transfer data from source to memory
        '''

        raise NotImplementedError("from_storage() is not defined!")

    def compress_memory(self, column, decompress=False):
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


class Pipe(object):
    '''
    a pipe connects two workspaces through their memory attribute

    Attributes:
        source (Workspace) instance of the data source
        destination (Workspace) instance of the data destination
    '''
    def __init__(self, source, destination):
        '''
        Args:
            source (Workspace) instance of a data source
            destination (Workspace) instance of a data destination
        '''
        self.source = source
        self.destination = destination

    def transfer(self, to='destination'):
        '''
        transfer data between memory attributes of the pipeline

        Args:
            to (str) either 'destination' or 'source'
        '''
        if to == 'destination':
            self.destination.memory = self.source.memory
        elif to == 'source':
            self.source.memory = self.destination.memory
        else:
            raise ValueError('{} is not a valid transfer direction'.format(to))


def in_batches(func):
    '''
    perform an operation in batches. the input function must return a bool that
    indicates whether the batch process is still ongoing

    Args:
        func (function) a function that processes an operation in batches.
            this function must return a bool (1 to continue|0 to stop)
    '''

    def wrapper(self, *args, **kwargs):

        flag = 1
        while flag is True:
            flag = func(self, *args, **kwargs)

        return flag
    return wrapper
