import unittest
import numpy as np

from os import mkdir
from shutil import rmtree
from subprocess import Popen, DEVNULL

from unittest import TestCase

from pandas import DataFrame

from dataspace.workspaces.remote_db import MongoFrame, remote_connection


test_frame = DataFrame(data=np.array([[1, 10, 'one'],
                                      [2, 20, 'two'],
                                      [3, 30, 'three']]),
                       columns=['feature a', 'feature b', 'name'])


class MongoFrameTest(TestCase, MongoFrame):

    @classmethod
    def setUpClass(self):

        # create directory for test database
        mkdir('./testdb')

        # start a mongod instance
        self.mongod = Popen(['mongod', '--dbpath', './testdb'], stdout=DEVNULL)

        # make MongoFrame attributes accessible
        MongoFrame.__init__(self, host='localhost', port=27017,
                            database='test_db', collection='test_collection',
                            authSource='admin', username=None, password=None)

        # save a copy of the original data
        self.original_data = test_frame

    def setUp(self):

        # make sure storage is empty
        self.delete_storage(clear_collection=True)

        # make sure original data is saved in memory
        self.memory = self.original_data

    def test_access_storage(self):

        # test for successfull connection
        @remote_connection
        def connection_exists(self):
            self.assertTrue(self.connection)
        connection_exists(self)

        # test for connection closure before raising error
        @remote_connection
        def throw_error(self):
            raise Exception
        try:
            throw_error(self)
        except Exception:
            self.assertFalse(self.connection)

    def test_load_to_memory(self):

        # add original data to storage
        self.to_storage(identifier=None)

        # test loading without find argument specified
        self.from_storage()
        self.assertTrue(self.original_data.equals(
            self.memory.drop('_id', axis=1)))

        # test loading with find agrument specified
        self.from_storage(filter={'name': 'one'})
        self.assertTrue(self.original_data.loc[
            self.original_data['name'] == 'one'].equals(
                self.memory.drop('_id', axis=1)))

    def test_to_storage(self):

        # should save no data (upsert=false)
        self.memory = self.original_data
        self.to_storage(identifier='name', upsert=False)
        self.from_storage()
        self.assertTrue(self.memory.empty)

        # should save data (upsert=true)
        self.memory = self.original_data
        self.to_storage(identifier='name', upsert=True)
        self.from_storage()
        self.assertTrue(self.original_data.equals(
            self.memory.drop('_id', axis=1)))

        # should save extra copy of data (identifier=None)
        self.memory = self.original_data
        self.to_storage(identifier=None)
        self.from_storage(filter={})
        self.assertTrue(len(self.memory) == (2 * len(self.original_data)))

    def test_delete_storage(self):

        # add original data to storage
        self.to_storage(identifier=None)

        # test remove select contents
        self.memory = self.original_data
        self.delete_storage(filter={'name': {'$in': ['one']}})
        self.from_storage()
        self.assertTrue(len(self.memory) == (len(self.original_data) - 1))

        # test protected remove all
        self.assertRaises(Exception, self.delete_storage)

        # test removing all
        self.delete_storage(clear_collection=True)
        self.from_storage()
        self.assertTrue(self.memory.empty)

    @classmethod
    def tearDownClass(self):

        # terminate the mongod instance
        self.mongod.terminate()
        self.mongod.wait()

        # delete the storage location
        rmtree('./testdb')


if __name__ == '__main__':
    unittest.main()
