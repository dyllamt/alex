import unittest

from unittest import TestCase
from pandas.util.testing import assert_frame_equal

from pandas import DataFrame

from dataspace.base import Workspace, Pipe, in_batches


initial_frame = DataFrame(data={'col1': [1, 2], 'col2': [3, 4]})


def test_func(a):
    '''
    test function for in_batches test. items are popped from a given list

    Args:
        a (list): a list, may be empty or non-empty

    Returns (bool): if a is empty, then returns False, else returns True
    '''

    a.pop()

    if not a:
        return False
    else:
        return True


class TestWorkspace(TestCase):
    '''
    test the base Workspace class
    '''

    def setUp(self):
        self.workspace = Workspace()

    def test_to_storage(self):
        self.assertRaises(NotImplementedError, self.workspace.to_storage)

    def test_from_storage(self):
        self.assertRaises(NotImplementedError, self.workspace.from_storage)

    def test_compress_memory(self):

        compressed_frame = DataFrame(
            data={'combined': [{'col1': 1, 'col2': 3},
                               {'col1': 2, 'col2': 4}]})
        final_frame = DataFrame(
            data={'combined.col1': [1, 2],
                  'combined.col2': [3, 4]})

        # load initial data
        self.workspace.memory = initial_frame

        # test whether compression works as intended
        self.workspace.compress_memory(column='combined', decompress=False)
        assert_frame_equal(self.workspace.memory, compressed_frame)

        # test whether decompression works as intended
        self.workspace.compress_memory(column='combined', decompress=True)
        assert_frame_equal(self.workspace.memory, final_frame)


class TestPipe(TestCase):
    '''
    test the base Pipe class
    '''

    def setUp(self):
        self.pipe = Pipe(source=Workspace(), destination=Workspace())

    def test_transfer(self):

        # load data in source
        self.pipe.source.memory = initial_frame
        self.assertIsNone(self.pipe.destination.memory)

        # transfer data from source to destination
        self.pipe.transfer(to='destination')
        assert_frame_equal(self.pipe.source.memory,
                           self.pipe.destination.memory)

        # transfer data from destination to source
        self.pipe.source.memory = None
        self.pipe.transfer(to='source')
        assert_frame_equal(self.pipe.source.memory,
                           self.pipe.destination.memory)

        # test that other directions raise error message
        self.assertRaises(ValueError, self.pipe.transfer, 'other')


class TestInBatches(TestCase):
    '''
    test the in_batches function
    '''

    def setUp(self):
        self.initial_list = [1, 2, 3, 4]

    def test_in_batches(self):
        decorated_func = in_batches(test_func)
        decorated_func(self.initial_list)

        # check that initial list is exausted
        self.assertFalse(self.initial_list)


if __name__ == '__main__':
    unittest.main()
