from simple_data_cleansing_classes.data import Data
from simple_data_cleansing_classes.project import Project
import pandas as pd
import os


class FindDuplicates:

    data = None  # Data for searching duplicates

    project = None  # Project where to store duplicates

    result_cols = None  # List of columns in result dataframe

    def __init__(self, project: Project, data: Data):

        self.data = data
        self.project = project
        # Files
        self.duples_wide_filename = "duples_wide.csv"
        self.duples_long_filename = "duples_long.csv"
        # Initial values of class variables
        self._duples_wide_data = None  # Duplicates data in wide format
        self._duples_long_data = None  # Duplicates data in long format
        # Clustered duplicates data
        self.duples_data = None
        self.clusters = None
        self.duplicates = None

    def process(self):
        """Searching, clustering and saving duplicates."""

        self.search_duplicates()
        self.add_duplicates()
        self.cluster_duplicates()
        self.make_wide()
        self.make_long()

    def search_duplicates(self):
        raise NotImplementedError

    def add_duplicates(self):
        self.data.data_norm['duplicates'] = self.duplicates
        self.data.data_norm.to_csv(self.data.data_norm_filepath, index=False)

    def make_wide(self):
        try:
            self._duples_wide_data = pd.merge(self.duples_data,
                                              self.data.data_norm[self.result_cols],
                                              on=self.data.id_column)
            self._duples_wide_data = pd.merge(self._duples_wide_data,
                                              self.data.data_norm[self.result_cols],
                                              left_on='duplicate_id', right_on=self.data.id_column,
                                              suffixes=('-src', '-dupl')) \
                .rename(columns={self.data.id_column + '-src': self.data.id_column}) \
                .drop([self.data.id_column + '-dupl'], axis=1)
            self._duples_wide_data.to_csv(os.path.join(self.project.duplicates_dir, self.duples_wide_filename))
        except Exception as e:
            raise Exception("Unable to create wide dataframe with duplicates!") from e

    def make_long(self):
        try:
            df_wide = self.duples_wide_data
            df_wide["id"] = df_wide.index
            df_long = pd.wide_to_long(df_wide,
                                      stubnames=self.result_cols[1:],
                                      sep='-',
                                      suffix=r'\w+',
                                      i=['duplicates_cluster', 'id'],
                                      j='source').reset_index().drop(["id"], axis=1)
            df_long.loc[df_long['source'] == 'dupl', self.data.id_column] = df_long['duplicate_id']
            df_long.drop(["source", "duplicate_id"], axis=1, inplace=True)
            df_long.drop_duplicates(subset=[self.data.id_column, 'duplicates_cluster'],
                                    keep='first',
                                    inplace=True)
            self._duples_long_data = df_long
            self._duples_long_data.to_csv(os.path.join(self.project.duplicates_dir, self.duples_long_filename))
        except Exception as e:
            raise Exception("Unable to create long dataframe with duplicates") from e

    def collect_duplicates_clusters(self, obj_id, cluster, df):
        res = df[df[self.data.id_column] == obj_id][[self.data.id_column, 'duplicate_id']]
        for r in res.itertuples(index=False):
            cluster.append(r[0])
            cluster.append(r[1])
            self.collect_duplicates_clusters(r[1], cluster, df)
        return sorted(list(set(cluster)))

    def cluster_duplicates(self):
        df_pairs = self.extract_pairs()
        clusters = []
        for row in df_pairs.sort_values(by=self.data.id_column).itertuples(index=False):
            if (len(clusters) > 0 and row[0] not in clusters[-1]) or len(clusters) == 0:
                cluster = self.collect_duplicates_clusters(row[1], [row[0], row[1]], df_pairs)
                clusters.append(cluster)
        for i, cl in enumerate(clusters):
            df_pairs.loc[df_pairs[self.data.id_column].isin(cl), 'duplicates_cluster'] = i
            df_pairs.loc[df_pairs['duplicate_id'].isin(cl), 'duplicates_cluster'] = i
        df_pairs['duplicates_cluster'] = df_pairs['duplicates_cluster'].astype(int)
        self.clusters = clusters
        self.duples_data = df_pairs

    def extract_pairs(self):
        if 'duplicates' in self.data.data_norm.columns:
            df_duples = self.data.data_norm[[self.data.id_column, 'duplicates']].copy()
            df_duples.fillna('', inplace=True)
            duplicates_list = list()
            for item in df_duples[[self.data.id_column, 'duplicates']].itertuples(index=False):
                item_matches = []
                duples_list = item[1] if type(item[1]) == list else eval(item[1])
                if len(duples_list) > 0:
                    for row in duples_list:  # eval
                        dupl_item = {self.data.id_column: row['source_id'],
                                     'duplicate_id': row['duplicate_id']}
                        del row['duplicate_id']
                        del row['source_id']
                        dupl_item['duplicates'] = row
                        item_matches.append(dupl_item)
                    if len(item_matches) > 0:
                        duplicates_list += item_matches
            return pd.DataFrame(duplicates_list)[[self.data.id_column, 'duplicate_id']]
        else:
            raise Exception("Unable to extract duplicates list!")

    @property
    def duples_wide_data(self):
        if self._duples_wide_data is None:
            try:
                self._duples_wide_data = pd.read_csv(self.duples_wide_filename,
                                                     converters=self.data.converters)
                self._duples_wide_data.fillna('', inplace=True)
            except:
                pass
        return self._duples_wide_data

    @property
    def duples_long_data(self):
        if self._duples_long_data is None:
            try:
                self._duples_long_data = pd.read_csv(self.duples_long_filename,
                                                     converters=self.data.converters)
                self._duples_long_data.fillna('', inplace=True)
            except:
                pass
        return self._duples_long_data
