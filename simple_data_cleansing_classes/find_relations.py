from abc import ABC

import pandas as pd
import os

try:
    from .data import Data
    from .project import Project
    from .finder import Finder
except:
    from simple_data_cleansing_classes.data import Data
    from simple_data_cleansing_classes.project import Project
    from simple_data_cleansing_classes.finder import Finder


class FindRelations(Finder, ABC):

    def __init__(self, project: Project, data_1: Data, data_2: Data,
                 data_1_output_columns, data_2_output_columns, **kwargs):

        Finder.__init__(self, project, **kwargs)
        self.data_1 = data_1  # First dataframe
        self.data_2 = data_2  # Second dataframe
        self.data_1_output_columns = data_1_output_columns
        self.data_2_output_columns = data_2_output_columns
        self._df_matches_wide = None
        self._df_matches_long = None
        self.matches_wide_filename = self.matches_column + '_wide.csv'
        self.matches_long_filename = self.matches_column + '_long.csv'

    def process(self):
        """Searching, clustering and saving duplicates."""

        self.process()
        self.make_wide()
        self.make_long()

    def make_wide(self):
        try:
            # Source data merging (data_1):
            self._df_matches_wide = pd.merge(self.df_matches_pairwise,
                                             self.data_1.data_norm[self.data_1_output_columns],
                                             left_on='source_id', right_on=self.data_1.id_column)
            # Target data merging (data_2):
            self._df_matches_wide = pd.merge(self._df_matches_wide,
                                             self.data_2.data_norm[self.data_2_output_columns],
                                             left_on='target_id', right_on=self.data_2.id_column,
                                             suffixes=('-src', '-trg')) \
                .rename(columns={self.data_1.id_column + '-src': self.data_id.id_column}) \
                .drop([self.data_2.id_column + '-trg'], axis=1)
            self._df_matches_wide.to_csv(os.path.join(self.project.matches_dir, self.matches_wide_filename))
        except Exception as e:
            raise Exception("Unable to create wide dataframe with matches!") from e

    def make_long(self):
        raise NotImplementedError

    @property
    def df_matches_wide(self):
        if self._df_matches_wide is None:
            try:
                self._df_matches_wide = pd.read_csv(self.matches_wide_filename,
                                                    converters=self.converters)
                self._df_matches_wide.fillna('', inplace=True)
            except:
                pass
        return self._df_matches_wide

    @property
    def df_matches_long(self):
        if self._df_matches_long is None:
            try:
                self._df_matches_long = pd.read_csv(self.matches_long_filename,
                                                    converters=self.converters)
                self._df_matches_long.fillna('', inplace=True)
            except:
                pass
        return self._df_matches_long
