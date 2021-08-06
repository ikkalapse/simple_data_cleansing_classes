from abc import ABC

import pandas as pd
import os

from .data import Data
from .project import Project
from .finder import Finder


class FindRelations(Finder, ABC):

    def __init__(self, project: Project, data_1: Data, data_2: Data,
                 data_1_output_columns, data_2_output_columns, **kwargs):

        Finder.__init__(self, project, data_1, data_2, **kwargs)
        # self.data_1 = data_1  # First dataframe
        # self.data_2 = data_2  # Second dataframe
        self.data_1_output_columns = data_1_output_columns
        self.data_2_output_columns = data_2_output_columns
        self._df_matches_wide = None
        self._df_matches_long = None
        self.matches_wide_filename = self.matches_column + '_wide.csv'
        self.matches_long_filename = self.matches_column + '_long.csv'

    def process(self):
        """Searching, clustering and saving duplicates."""

        Finder.process(self)
        self.make_long()

    def make_long(self):
        try:
            df_wide = self.df_matches_wide
            df_wide["id"] = df_wide.index
            df_long = pd.wide_to_long(df_wide,
                                      stubnames=[col for col in self.data_1_output_columns
                                                 if col != self.data_1.id_column],
                                      sep='-',
                                      suffix=r'\w+',
                                      i=[self.clusters_column, 'id'],
                                      j='source') \
                .reset_index() \
                .drop(["id"], axis=1)
            df_long.loc[df_long['source'] == 'trg', self.data_1.id_column] = df_long['target_id']
            df_long.drop(["source", "target_id"], axis=1, inplace=True)
            df_long.drop_duplicates(subset=[self.data_1.id_column, self.clusters_column],
                                    keep='first',
                                    inplace=True)
            self._df_matches_long = df_long
            self._df_matches_long.to_csv(os.path.join(self.project.project_dir, self.matches_long_filename))
        except Exception as e:
            raise Exception("Unable to create long dataframe!") from e
