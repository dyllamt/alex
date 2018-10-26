from matexplorer.workspaces.base import Workspace
from matminer.data_retrieval.retriev_MP import MPDataRetrieval
'''
retrieval workspaces are connected to external data sources. current retrieval
workspaces are wrappers for childeren of the matminer BaseDataRetrieval class
'''


class MPFrame(Workspace, MPDataRetrieval):
    '''
    wrapper for making MPDataRerieval a Workspace
    '''

    def __init__(self, api_key=None):
        Workspace.__init__(self)
        MPDataRetrieval.__init__(self, api_key=api_key)

    def to_storage(self):
        '''
        transfer data from memory to storage
        '''

        raise NotImplementedError("to_source() is not defined!")

    def from_storage(self, criteria, properties, index_mpid=False, kwargs):
        '''
        transfer data from storage to memory. see matminer's get_dataframe
        method for optional arguments to pass to get_datafraem
        '''

        self.memory = self.get_dataframe(criteria, properties,
                                         index_mpid=index_mpid, **kwargs)
