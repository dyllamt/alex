from alex.workspaces.base import MongoFrame

from matminer.data_retrieval.retrieve_MP import MPDataRetrieval
from matminer.featurizers.structure import SiteStatsFingerprint

from pymatgen.core.structure import Structure
from pandas import concat

'''
this module creates workspaces for managing local databases from datastreams
such as the Materials Project or the Open Quantum Materials Database.
'''

STANDARD_FEAT = SiteStatsFingerprint.from_preset(
    preset='CrystalNNFingerprint_ops',
    stats=['mean', 'std_dev', 'maximum', 'minimum'])


class MPStructureSpace(MPDataRetrieval, MongoFrame):
    '''
    the structures in the materials project are collectable through the
    matminer MPDataRetrieval class. combining the data retrieval class with a
    MongoFrame enables storing this data in a local mongodb.

    Attributes:

        base workspace attributes:
        . path (str) path to local mongodb
        . database (str) name of the database
        . collection (str) name of the collection
        . connection (Collection|None) statefull pymongo connection to storage
        . memory (DataFrame|None) temporary data storage
        . api_key (str) your Materials Project api key

    '''
    def __init__(self, path, database, collection, api_key=None):
        '''
        Args:
            path (str) path to a mongodb directory
            database (str) name of a pymongo database
            collection (str) name of a pymongo collection
            api_key (str) your Materials Project api key
        '''

        MongoFrame.__init__(self, path, database, collection)
        MPDataRetrieval.__init__(self, api_key)

    def update_ids(self):
        '''
        update the current list of materials ids in the database
        '''

        # gather materials project data in memory
        self.memory = self.get_dataframe(criteria={'structure':
                                                   {'$exists': True}},
                                         properties=['material_id'],
                                         index_mpid=False)

        # store data in permanant storage
        self.save_to_storage(identifier='material_id', upsert=True)

    def update_structures(self, batch_size=500):
        '''
        update structures in batches to accomodate the api limits. also collect
        basic information such as formation energy and number of sites.

        Args:
            batch_size (int) limit on number of structures retrieved at a time
        '''

        collection_count = 0
        while True:

            # load material ids without structure to memory
            self.load_to_memory(find={'filter':
                                      {'structure': {'$exists': False}},
                                      'projection':
                                      {'material_id': 1},
                                      'limit': batch_size})
            mp_ids = list(self.memory['material_id'].values)

            # end loop if there are no more structures to gather
            if len(mp_ids) == 0:
                break

            # save materials data to local database
            self.memory = self.get_dataframe(
                criteria={'material_id': {'$in': mp_ids}},
                properties=['material_id', 'structure',
                            'formation_energy_per_atom', 'e_above_hull',
                            'nsites'],
                index_mpid=False)
            self.save_to_storage(identifier='material_id', upsert=True)

            # report collection status
            collection_count += len(mp_ids)
            print('collected {} structures'.format(collection_count))

    def featurize_structures(self, featurizer=STANDARD_FEAT, batch_size=500):
        '''
        featurize structures in the local database in batches

        Args:
            featurizer (BaseFeaturizer) an instance of a structural featurizer
            batch_size (int) limit on number of structures retrieved at a time
        '''

        featurize_count = 0
        while True:

            # load structures that are not featurized from storage
            self.load_to_memory(find={'filter': {'structure':
                                                 {'$exists': True},
                                                 'structure_features':
                                                 {'$exists': False}},
                                      'projection': {'material_id': 1,
                                                     'structure': 1,
                                                     '_id': 0},
                                      'limit': batch_size})
            self.memory['structure'] =\
                [Structure.from_dict(struct) for struct in
                    self.memory['structure']]

            # end loop if there are no more structures to featurize
            if len(self.memory) == 0:
                break

            # featurize loaded structures
            featurizer.featurize_dataframe(self.memory, 'structure',
                                           ignore_errors=True, pbar=False)

            # store compressed features in permanant storage
            mp_ids = self.dataframe[['material_id']]
            self.memory.drop(columns=['material_id', 'structure'],
                             inplace=True)
            self.memory_compression('structure_features')
            self.memory = concat([mp_ids, self.dataframe], axis=1)
            self.save_to_storage(identifier='material_id', upsert=True)

            # report featurization status
            featurize_count += len(self.memory)
            print('collected {} structures'.format(featurize_count))
