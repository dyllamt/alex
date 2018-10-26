
'''
this module defines key objects for data exploration:

1. Workspace - an object for handeling structured data. a workspace always has
    a memory attribute for storing data during run-time, but may also have
    connections to other data sources such as APIs or local databases.

2. Task - an object for performing operations within a Workspace. a Task
    inherits from the Workspace that it operates within.

3. Pipe - an object for passing data between two Workspace instances. data is
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

        raise NotImplementedError("to_source() is not defined!")

    def from_storage(self):
        '''
        transfer data from source to memory
        '''

        raise NotImplementedError("from_source() is not defined!")


class Task(object):
    '''
    a worker will execute a series of operations. an example is loading and
    featurizing data from the materials project in batches. a worker operates
    inside a single workspace. he/she can not escape their space!
    '''
    def __init__(self, workspace, task):
        '''
        Args:
            workspace (Workspace) instance of a Workspace to execute a task in
            task (function) a procedural function to execute. this function can
                have optional arguments set with *args and **kwargs
        '''
        self.workspace = workspace
        self.task = task

    def execute(self, *args, **kwargs):
        '''
        execute the task assigned to this worker. optional arguments can be
        passed into the task by *args and **kwargs
        '''
        self.task(*args, **kwargs)


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

