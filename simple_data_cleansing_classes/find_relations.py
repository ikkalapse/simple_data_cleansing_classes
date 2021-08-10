from abc import ABC

from .data import Data
from .project import Project
from .finder import Finder


class FindRelations(Finder, ABC):

    def __init__(self, project: Project, data_1: Data, data_2: Data,
                 data_1_output_columns, data_2_output_columns, **kwargs):

        Finder.__init__(self, project, data_1, data_2, **kwargs)
        self.data_1_output_columns = data_1_output_columns
        self.data_2_output_columns = data_2_output_columns
        self._df_matches_wide = None
        self._df_matches_long = None
        self.matches_wide_filename = self.matches_column + '_wide.csv'
        self.matches_long_filename = self.matches_column + '_long.csv'
