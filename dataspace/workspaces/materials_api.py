from workspace.base import Workspace

from matminer.data_retrieval.retrieve_MP import MPDataRetrieval

'''
this module implements workspaces that handle structured data in materials
database that is serviced by an api. the workspaces are essentially wrappers
around childeren of the matminer BaseDataRetrieval class
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

        raise NotImplementedError("to_storage() is not defined!")

    def from_storage(self, criteria, properties, index_mpid=False, **kwargs):
        '''
        transfer data from storage to memory. see the matminer MPDataRetrieval
        class for optional arguments to pass to the get_dataframe method
        '''

        self.memory = self.get_dataframe(criteria, properties,
                                         index_mpid=index_mpid, **kwargs)
