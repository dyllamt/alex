from dataspace.base import Workspace

from matminer.data_retrieval.retrieve_base import BaseDataRetrieval

"""Implements workspaces that handle structured data in materials databases
that are serviced by APIs. The workspaces are essentially wrappers around
children of the matminer BaseDataRetrieval class.
"""


class APIFrame(Workspace):
    """Wrapes BaseDataRetrieval sub-classes from matminer.
    """
    def __init__(self, RetrievalSubClass, **kwargs):
        """Initializes an istance from a BaseDataRetrieval subclass.

        Args:
            RetrievalSubClass: (BaseDataRetrieval) A subclass which defines a
                get_dataframe method for retrieving data from an API.
            kwargs: Optional keyword arguments used to construct an instance of
                the RetrievalSubClass class.
        """
        if not issubclass(RetrievalSubClass, BaseDataRetrieval):
            raise Exception(
                'The retriever must be an a subclass of BaseDataRetrieval!')
        else:
            Workspace.__init__(self)
            self.connection = RetrievalSubClass(**kwargs)

    def to_storage(self):
        """Transfers data from memory to storage.
        """
        raise NotImplementedError("to_storage() is not defined!")

    def from_storage(self, **kwargs):
        """Collects data from storage into memory.

        Args:
            kwargs: Keyword arguments passed to the get_dataframe method.
        """
        self.memory = self.connection.get_dataframe(**kwargs)
