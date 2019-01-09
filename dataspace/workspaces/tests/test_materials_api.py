import unittest

from unittest import TestCase, SkipTest
from pandas.util.testing import assert_frame_equal

from pandas import DataFrame

from dataspace.workspaces.materials_api import MPFrame


test_frame = DataFrame(data={'material_id': ['mp-23']})


class TestMPFrame(TestCase):
    '''
    test the MPFrame class
    '''

    def setUp(self):
        self.workspace = MPFrame()

    def test_to_storage(self):
        self.assertRaises(NotImplementedError, self.workspace.to_storage)

    def test_from_storage(self):
        if self.workspace.mprester.api_key:
            self.workspace.from_storage(criteria={'material_id': 'mp-23'},
                                        properties=['material_id'])
            assert_frame_equal(self.workspace.memory, test_frame)
        else:
            raise SkipTest('Skipped MPFrame test; no MPI_KEY detected')


if __name__ == '__main__':
    unittest.main()
