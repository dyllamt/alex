import unittest

from unittest import TestCase

from pandas import DataFrame
from pandas.util.testing import assert_frame_equal

from matminer.data_retrieval.retrieve_AFLOW import AFLOWDataRetrieval

from dataspace.workspaces.materials_api import APIFrame


test_frame = DataFrame(data={'material_id': ['mp-23']})


class TestAPIFrame(TestCase):
    """Tests the APIFrame class.
    """

    def setUp(self):
        self.workspace = APIFrame(AFLOWDataRetrieval)

    def test_to_storage(self):
        self.assertRaises(NotImplementedError, self.workspace.to_storage)

    def test_from_storage(self):
        self.workspace.from_storage(criteria={'material_id': 'mp-23'},
                                    properties=['material_id'])
        assert_frame_equal(self.workspace.memory, test_frame)


if __name__ == '__main__':
    unittest.main()
