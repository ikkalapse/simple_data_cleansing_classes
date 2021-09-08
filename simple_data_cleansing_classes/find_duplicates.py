from abc import ABC

from .data import Data
from .project import Project
from .finder import Finder
import pandas as pd
import os


class FindDuplicates(Finder, ABC):
    data = None  # Data for searching duplicates

    project = None  # Project where to store duplicates

    result_cols = None  # List of columns in result dataframe

    clusters_column = 'matches_cluster'

    def __init__(self, project: Project, data: Data, **kwargs):

        Finder.__init__(self, project, data, **kwargs)
        # self.data_1 = data
        self.data_1_output_columns = kwargs.get("data_1_output_columns", list(self.data_1.data.columns))
        self._df_matches_wide = None
        self._df_matches_long = None
        self._matches_clusters = None
        self.matches_wide_filename = self.matches_column + '_wide.csv'
        self.matches_long_filename = self.matches_column + '_long.csv'
        self.clusters_column = kwargs.get("clusters_column", self.clusters_column)

    def process(self):
        """Searching, clustering and saving duplicates."""

        Finder.process(self)
        self.make_long()

    def make_wide(self):
        self.make_clusters()
        Finder.make_wide(self)

    def _collect_clusters(self, obj_id, cluster, df):
        res = df[df['source_id'] == obj_id][['source_id', 'target_id']]
        for r in res.itertuples(index=False):
            cluster.append(r[0])
            cluster.append(r[1])
            self._collect_clusters(r[1], cluster, df)
        return sorted(list(set(cluster)))

    def make_clusters(self):
        """Set cluster id for every row in pairwise dataframe."""

        df_pairs = self.df_matches_pairwise
        clusters = []
        for row in df_pairs[['source_id', 'target_id', 'info']].sort_values(by='source_id').itertuples(index=False):
            if (len(clusters) > 0 and row[0] not in clusters[-1]) or len(clusters) == 0:
                cluster = self._collect_clusters(row[1], [row[0], row[1]], df_pairs)
                clusters.append(cluster)
        for i, cl in enumerate(clusters):
            df_pairs.loc[df_pairs['source_id'].isin(cl), self.clusters_column] = i
            df_pairs.loc[df_pairs['target_id'].isin(cl), self.clusters_column] = i
        df_pairs[self.clusters_column] = df_pairs[self.clusters_column].astype(int)
        df_pairs.to_csv(self.matches_pairwise_filename, index=False)

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

    '''def make_long(self):
        try:
            df_1_id_col = self.data_1.id_column
            df_2_id_col = df_1_id_col if len(self.data) == 1 else self.data_2.id_column
            df_id_cols = [df_1_id_col, df_2_id_col]
            df_id_cols = list(set(df_id_cols))  # only unique values

            df_wide = self.df_matches_wide
            df_wide["id"] = df_wide.index
            df_long = pd.wide_to_long(df_wide,
                                      stubnames=[col for col in self.data_1_output_columns
                                                 if col not in df_id_cols],
                                      sep='-',
                                      suffix=r'\w+',
                                      i=['id'],
                                      j='source') \
                .reset_index() \
                .drop(["id"], axis=1)
            self._df_matches_long = df_long
            self.save_long()
        except Exception as e:
            raise Exception("Unable to create long dataframe!") from e'''

    def save_long(self):
        self._df_matches_long.to_csv(os.path.join(self.project.project_dir,
                                                  self.matches_long_filename))

    @property
    def df_matches_long(self):
        """Return pandas dataframe contains matches."""

        if self._df_matches_long is None and os.path.isfile(self.matches_long_filename):
            try:
                self._df_matches_long = pd.read_csv(self.matches_wide_filename,
                                                    converters=self.converters)
                self._df_matches_long.fillna('', inplace=True)
            except Exception as e:
                raise Exception("Unable to read matches wide dataframe!") from e
        return self._df_matches_long
