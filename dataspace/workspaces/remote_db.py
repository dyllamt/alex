from dataspace.base import Workspace

from pandas import DataFrame

from pymongo import MongoClient

'''
this module implements workspaces that handle structured data in remote
databases. currently, mongodb is supported through the pymongo interface
'''


def remote_connection(func):
    '''
    make a remote connection to storage around an operation. errors encountered
    while the connection is active are raised after terminating the connection.

    Args:
        func (function) a function that requires access to a remote mongodb
    '''

    def wrapper(self, *args, **kwargs):

        # set-up a database connection
        self.connection = MongoClient(
            self.host, self.port, authSource=self.authSource,
            username=self.username, password=self.password)
        print('connected to {} @ {}'.format(self.database, self.host))

        # execute operation
        error = None
        try:  # attempt to execute operation
            result = func(self, *args, **kwargs)
        except Exception as e:  # put off raising errors till later
            error = e

        # close the database connection
        self.connection.close()
        self.connection = None
        print('disconnected \n')

        if error:  # errors in execution are raised after disconnection
            raise error

        return result

    return wrapper


class MongoFrame(Workspace):
    '''
    abstraction for structured data in pymongo Collections (storage) and pandas
    DataFrames (memory). methods support data transfers between storage and
    memory. storage and memory are treated as instance attributes. the index
    attribute of DataFrames are not stored or preserved in compression. you
    must add the index as a column if you want to store that information!

    Attributes:
        host (str) hostname or IP address or Unix domain socket path
        port (int) port number on which to connect
        database (str) storage database
        collection (str) storage collection
        authSource (str) database to authenticate against
        username (str|None) username to authenticate with
        password (str|None) password to authenticate with
        connection (MongoClient|None) statefull connection to storage
        memory (DataFrame|None) pandas dataframe for temporary storage
    '''

    def __init__(self, host, port, database, collection, authSource=None,
                 username=None, password=None):
        '''
        Args:
            host (str) hostname or IP address or Unix domain socket path
            port (int) port number on which to connect
            database (str) storage database
            collection (str) storage collection
            authSource (str|None) database to authenticate against
                if None, defaults to the storage database
            username (str|None) authentication username
            password (str|None) authentication password
        '''
        Workspace.__init__(self)
        self.host = host
        self.port = port
        self.database = database
        self.collection = collection
        self.authSource = authSource or database
        self.username = username
        self.password = password

    @remote_connection
    def to_storage(self, identifier, upsert=True):
        '''
        save data in memory (DataFrame) to storage (Collection)

        Args:
            identifier (str|None) document field (column) of unique identifier.
                if None then unique insertion is not enforced
            upsert (bool) insert missing documents in unique insertion mode
        '''

        collection = self.connection[self.database][self.collection]

        if identifier:  # unique insertion mode
            for row in self.memory.to_dict(orient='records'):
                collection.update_one(
                    filter={identifier: row[identifier]},
                    update={'$set': row},
                    upsert=upsert)
        else:  # documents are non-unique
            collection.insert_many(
                self.memory.to_dict(orient='records'))

    @remote_connection
    def from_storage(self, **find):
        '''
        load data from storage (Collection) to memory (DataFrame)

        args:
            **find (dict) optional arguments to pass to pymongo.find
        '''

        collection = self.connection[self.database][self.collection]

        self.memory = DataFrame.from_records(
            list(collection.find(**find)))

    @remote_connection
    def delete_storage(self, filter={}, clear_collection=False):
        '''
        delete collection documents with a pymongo query operator

        Args:
            filter (son) pymongo query operator passed to delete_many()
            clear_collection (bool) clear storage entirely
        '''

        collection = self.connection[self.database][self.collection]

        if clear_collection:  # remove all documents
            collection.delete_many({})
        elif filter:  # remove documents matching kwargs
            collection.delete_many(filter)
        else:  # make sure collection purge is intended
            raise Exception('Do you mean to delete everything in {}.{}? If so,'
                            'then flag clear_collection as True.'.format(
                                self.database, self.collection))
