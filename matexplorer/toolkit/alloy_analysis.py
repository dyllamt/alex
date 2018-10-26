from matexplorer.workspaces.data_streams import MPStructureSpace
from matexplorer.workspaces.data_analysis import AlloyAnalysis

'''
this module creates pipelines for exploring alloy systems in materials
data. pipelines connect several different input datastreams (the Materials
Project and/or the Open Quantum Materials Database) and analysis tools.
'''


class MPAlloyExplorer(MPStructureSpace, AlloyAnalysis):
    '''
    explore alloy spaces using the set of structures in the Materials
    Project. See MPStructureSpace for methods to handle the incoming data
    stream. See AlloyAnalysis for methods to perform alloy analysis.
    '''

    def __init__(self, path, database, collection, api_key=None):
        MPStructureSpace.__init__(self, path, database, collection, api_key)

    def update_local_dataset(self):
        '''
        convienience function to update all of the data in local storage
        '''
        self.update_ids()
        self.update_structures()
        self.featurize_structures()


if __name__ == '__main__':
    # from pymatgen.core.structure import Structure

    # database information
    PATH = '/home/mdylla/repos/code/orbital_phase_diagrams/local_db'
    DATABASE = 'orbital_phase_diagrams'
    COLLECTION = 'structure'
    API_KEY = 'VerGNDXO3Wdt4cJb'

    # initial material id
    INITIAL_ID = 'mp-961652'

    exp = MPAlloyExplorer(path=PATH, database=DATABASE, collection=COLLECTION,
                          api_key=API_KEY)
    exp.featurize_structures()
    # exp.load_to_memory(find={'filter': {'material_id': INITIAL_ID},
    #                          'projection': {'structure': 1,
    #                                         '_id': 0}})
    # parent = Structure.from_dict(exp.memory['structure'][0])

    # exp.visulalize_alloy_space(parent)
