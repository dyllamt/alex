
from matexplorer.base import Pipe
from matexplorer.workspaces.retrievers import MPFrame
from matexplorer.workspaces.local import MongoFrame

from pandas import DataFrame, concat

from sklearn.decomposition import PCA

from pymatgen.analysis.defects.generators import VacancyGenerator
from pymatgen.core.structure import Structure

from matminer.featurizers.site import CrystalNNFingerprint
from matminer.featurizers.structure import SiteStatsFingerprint

import numpy as np
import plotly.graph_objs as go
import plotly.offline as py

'''
load structures from materials project to perform alloy analysis. structures
and their features are stored in a local mongo database

TODO:
    . what is the best set of structural feature vectors for matching?
    . what is the best distance metric for finding similar structures?
'''

# user variables
PATH = '/home/mdylla/repos/code/orbital_phase_diagrams/local_db'
DATABASE = 'orbital_phase_diagrams'
COLLECTION = 'structure'
API_KEY = 'VerGNDXO3Wdt4cJb'

FRAMEWORK_FEAT = SiteStatsFingerprint(
    site_featurizer=CrystalNNFingerprint.from_preset(
        preset="ops", distance_cutoffs=None, x_diff_weight=0.0,
        porous_adjustment=False),
    stats=['mean', 'std_dev', 'maximum', 'minimum'])


class GenerateMPStructureSpace(Pipe):
    '''
    pipeline for generating a structure space on your local machine
    '''
    def __init__(self, path, database, collection, api_key=None):
        '''
        Args:
            path (str) path to a local mongodb directory
            database (str) name of a pymongo database
            collection (str) name of a pymongo collection
            api_key (str) materials project api key
        '''
        self.source = MPFrame(api_key=api_key)
        self.destination = MongoFrame(path=path, database=database,
                                      collection=collection)

    def update_all(self, batch_size=500, featurizer=FRAMEWORK_FEAT):
        '''
        convienience function for updating the structure space
        '''
        self.update_mp_ids()
        self.update_structures(batchsize=500)
        self.featurize_structures(batch_size=batch_size, featurizer=featurizer)

    def update_mp_ids(self):
        '''
        update the collection of mp ids in the local database
        '''
        self.source.from_storage(criteria={'structure': {'$exists': True}},
                                 properties=['material_id'],
                                 index_mpid=False)
        self.transfer(to='destination')
        self.destination.to_storage(identifier='material_id', upsert=True)

    def update_structures(self, batch_size=500):
        '''
        update the collection of structures in the local database. updates are
        done in batches to accomadate the api limits of the materials project

        Args:
            batch_size (int) limit on number of structures retrieved at a time
        '''

        collection_count = 0
        while True:

            # load material ids without structure to memory
            self.destination.from_storage(find={'filter':
                                                {'structure':
                                                 {'$exists': False}},
                                                'projection':
                                                {'material_id': 1},
                                                'limit': batch_size})
            # end loop if there are no more structures to gather
            if len(self.destination.memory.index) == 0:
                break
            else:
                mp_ids = list(self.destination.memory['material_id'].values)

            # retrieve materials data from mp database
            self.source.from_storage(
                criteria={'material_id': {'$in': mp_ids}},
                properties=['material_id', 'structure',
                            'formation_energy_per_atom', 'e_above_hull',
                            'nsites'],
                index_mpid=False)

            # save materials data to local storage
            self.transfer(to='destination')
            self.destination.to_storage(identifier='material_id', upsert=True)

            # report collection status
            collection_count += len(mp_ids)
            print('collected {} structures'.format(collection_count))

    def featurize_structures(self, batch_size=500, featurizer=FRAMEWORK_FEAT):
        '''
        featurize structures in the local database in batches

        Args:
            batch_size (int) limit on number of structures retrieved at a time
            featurizer (BaseFeaturizer) an instance of a structural featurizer
        '''

        featurize_count = 0
        while True:

            # load structures that are not featurized from storage
            self.destination.from_storage(
                find={'filter': {'structure': {'$exists': True},
                      'structure_features': {'$exists': False}},
                      'projection': {'material_id': 1,
                                     'structure': 1,
                                     '_id': 0},
                      'limit': batch_size})
            # end loop if there are no more structures to gather
            if len(self.destination.memory.index) == 0:
                break
            else:
                self.memory['structure'] =\
                    [Structure.from_dict(struct) for struct in
                        self.memory['structure']]

            # featurize loaded structures
            featurizer.featurize_dataframe(self.destination.memory,
                                           'structure',
                                           ignore_errors=True,
                                           pbar=False)

            # store compressed features in permanant storage
            mp_ids = self.destionation.memory[['material_id']]
            self.destination.memory.drop(columns=['material_id', 'structure'],
                                         inplace=True)
            self.destination.memory_compression('structure_features')
            self.destination.memory = concat([mp_ids, self.memory], axis=1)
            self.destination.to_storage(identifier='material_id', upsert=True)

            # report featurization status
            featurize_count += len(self.destination.memory)
            print('featurized {} structures'.format(featurize_count))


class AnalyzeAlloySpace(MongoFrame):
    '''
    a substitutional alloy space contains structures with the same sites or
    structures related by either a single vacancy or intersticial defect. a
    structure prototype is used to expand an alloy space for a material family.
    similar structures to the prototype and its defect structures (exemplars)
    are found using a distance metric of structural feature vectors, which are
    stored in a local database of structures. the neighborhoods of compounds
    that are similar to the exemplars can be analyzed using heirarchical
    principle component analysis, which visualizes the maximum variance in and
    among the neighborhoods of structures.

    local structure database must contain "material_id", "structure", and
    "structure_features" fields. MPStructureSpace constructs a compatible
    database for this workspace, but could be extended to the OQMD structures

    Attributes:

        alloy attributes
        . parent (Structure) pymatgen object that alloy space is built around
        . exemplars (DataFrame) contains both pymatgen Structures and their
            feature vectors. an exemplar (row of DataFrame) is a structure that
            is either the parent structure or a vacancy/intersticial structure.
            the first row is the exemplar of the parent structure
        . distances (DataFrame) pairwise distances matrix of the database
            structures (row) and exemplars (column)
        . neighborhoods ([DataFrame]) the index contains the unique database
            entry identifier, the data contains the feature vectors of
            each structure in the neighborhood of each exemplar (list order is
            preserved from row order of exemplars attribute)

        inherited from MongoFrame:
        . path (str) path to local mongodb
        . database (str) name of the database
        . collection (str) name of the collection
        . connection (Collection|None) statefull pymongo connection to storage
        . memory (DataFrame|None) temporary data storage
    '''

    def __init__(self, path, database, collection):
        '''
        Args:
            path (str) path to a mongodb directory
            database (str) name of a pymongo database
            collection (str) name of a pymongo collection
        '''
        MongoFrame.__init__(self, path, database, collection)
        self.parent = None
        self.exemplars = None
        self.distances = None
        self.neighborhoods = None

    def visulalize_alloy_space(self, parent, threshold=0.1):
        '''
        perform principle component analysis on structures that are within the
        neighborhood of the parent and defect structures set by threshold
        '''
        self.parent = parent
        self.exemplars = self.generate_exemplars()
        self.distances = self.generate_distances()
        self.neighborhoods = self.generate_neighborhoods(threshold)
        self.memory = None  # clear large ammount of data in temporary storage

        combined_neighborhoods = concat(self.neighborhoods, axis=0)
        pca = PCA(n_components=2, whiten=True)
        pca.fit(combined_neighborhoods)

        traces = []
        for neighborhood in self.neighborhoods:
            components = pca.transform(neighborhood)
            traces.append(go.Scatter(x=components[:, 0],
                                     y=components[:, 1],
                                     text=neighborhood.index.values,
                                     mode='markers'))
        fig = go.Figure(data=traces)
        py.plot(fig, filename='test_figure')

    def generate_exemplars(self):
        '''
        generate feature vectors of parent compound and its defect structures

        Returns (DataFrame) exemplar structures and their feature vectors. the
            first row is the parent, the remaining are defect structures
        '''

        # generate exemplar structures
        exemplars = [self.parent]
        vgen = VacancyGenerator(structure=self.parent)
        exemplars.extend([i.generate_defect_structure(supercell=(1, 1, 1))
                          for i in vgen])

        # featurize exemplar structures
        featurizer = SiteStatsFingerprint.from_preset(
            preset='CrystalNNFingerprint_ops',
            stats=['mean', 'std_dev', 'maximum', 'minimum'])
        exemplars = DataFrame.from_dict({'structure': exemplars})
        featurizer.featurize_dataframe(exemplars, 'structure',
                                       ignore_errors=False, pbar=False)
        return exemplars

    def generate_distances(self):
        '''
        generate pairwise distances between database structures and exemplars

        Returns (DataFrame) distances between database structures (rows) and
            and the instance exemplars (columns)
        '''

        # load structural features from database of structures
        self.load_to_memory(find={'filter':
                                  {'structure_features': {'$exists': True}},
                                  'projection':
                                  {'material_id': 1,
                                   'structure_features': 1,
                                   '_id': 0}
                                  })
        self.memory_compression(column='structure_features', decompress=True)
        self.memory.set_index('material_id', inplace=True)
        self.memory.sort_index(axis=1, inplace=True)
        database_features = self.memory.values

        # compute pairwise distances with numpy broadcasting
        exemplar_features = self.exemplars.drop(
            columns=['structure']).sort_index(axis=1).values
        distances = np.sqrt(
            np.sum((database_features[:, np.newaxis, :] -
                    exemplar_features[np.newaxis, :, :]) ** 2., axis=2))

        # return result as a dataframe
        return DataFrame(data=distances, index=self.memory.index.values,
                         columns=self.exemplars.index.values.astype(str))

    def generate_neighborhoods(self, threshold):
        '''
        generate neighborhoods around each exemplar within threshold distance.
        the data in self.memory is reused from the generate_distances()
        function call durring instance initiation

        Returns ([DataFrame]) a list of neighborhoods. each neighborhood
            contains the mp ids (index) and the feature vectors (data)
        '''

        distances = self.distances.values

        neighborhoods = []
        for j in range(distances.shape[1]):
            distances_from_exemplar_j = distances[:, j]
            bool_within_threshold = distances_from_exemplar_j <= threshold
            neighborhoods.append(
                self.memory.loc[bool_within_threshold])
        return neighborhoods
