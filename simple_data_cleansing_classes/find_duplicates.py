import os
from datetime import datetime
import pandas as pd


class FindDuplicates:

    obj_id_col = 'object_id'  # Name of column with ID of objects

    result_cols = None  # List of columns in result dataframe

    converters = None  # Converters for reading data using Pandas

    def __init__(self, **kwargs):
        # Project name
        self.project = kwargs.get('project', datetime.now().strftime("%Y-%m-%d_%H%M"))
        self.converters = kwargs.get('converters', self.converters)
        self.obj_id_col = kwargs.get('obj_id_col', self.obj_id_col)
        # Directories
        self.project_dir = os.path.join('projects', self.project)
        self.datasets_dir = os.path.join(self.project_dir, 'datasets')
        self.duplicates_dir = os.path.join(self.project_dir, 'duplicates')
        os.makedirs(self.project_dir, exist_ok=True)
        os.makedirs(self.datasets_dir, exist_ok=True)
        os.makedirs(self.duplicates_dir, exist_ok=True)
        # Files
        self.dirty_filename = "dirty.csv"
        self.dirty_norm_filename = "dirty_norm.csv"
        self.duples_wide_filename = "duples_wide.csv"
        self.duples_long_filename = "duples_long.csv"
        self.dirty_filepath = os.path.join(self.datasets_dir, self.dirty_filename)
        self.dirty_norm_filepath = os.path.join(self.datasets_dir, self.dirty_norm_filename)
        # Initial values of class variables
        self._dirty_data = None  # Dirty data
        self._dirty_norm_data = None  # Normalized dirty data
        self.duples_data = None  # Clustered duplicates data
        self._duples_wide_data = None  # Duplicates data in wide format
        self._duples_long_data = None  # Duplicates data in long format
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

    def prepare_data(self):
        raise NotImplementedError

    def read_data(self):
        raise NotImplementedError

    def add_duplicates(self):
        self._dirty_norm_data['duplicates'] = self.duplicates
        self._dirty_norm_data.to_csv(self.dirty_norm_filepath, index=False)

    def make_wide(self):
        try:
            self._duples_wide_data = pd.merge(self.duples_data,
                                              self.dirty_norm_data[self.result_cols],
                                              on=self.obj_id_col)
            self._duples_wide_data = pd.merge(self._duples_wide_data,
                                              self.dirty_norm_data[self.result_cols],
                                              left_on='duplicate_id', right_on=self.obj_id_col,
                                              suffixes=('-src', '-dupl')) \
                .rename(columns={self.obj_id_col + '-src': self.obj_id_col}) \
                .drop([self.obj_id_col + '-dupl'], axis=1)
            self._duples_wide_data.to_csv(os.path.join(self.duplicates_dir, self.duples_wide_filename))
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
            df_long.loc[df_long['source'] == 'dupl', self.obj_id_col] = df_long['duplicate_id']
            df_long.drop(["source", "duplicate_id"], axis=1, inplace=True)
            df_long.drop_duplicates(subset=[self.obj_id_col, 'duplicates_cluster'],
                                    keep='first',
                                    inplace=True)
            self._duples_long_data = df_long
            self._duples_long_data.to_csv(os.path.join(self.duplicates_dir, self.duples_long_filename))
        except Exception as e:
            raise Exception("Unable to create long dataframe with duplicates") from e

    def collect_duplicates_clusters(self, obj_id, cluster, df):
        res = df[df[self.obj_id_col] == obj_id][[self.obj_id_col, 'duplicate_id']]
        for r in res.itertuples(index=False):
            cluster.append(r[0])
            cluster.append(r[1])
            self.collect_duplicates_clusters(r[1], cluster, df)
        return sorted(list(set(cluster)))

    def cluster_duplicates(self):
        df_pairs = self.extract_pairs()
        clusters = []
        for row in df_pairs.sort_values(by=self.obj_id_col).itertuples(index=False):
            if (len(clusters) > 0 and row[0] not in clusters[-1]) or len(clusters) == 0:
                cluster = self.collect_duplicates_clusters(row[1], [row[0], row[1]], df_pairs)
                clusters.append(cluster)
        for i, cl in enumerate(clusters):
            df_pairs.loc[df_pairs[self.obj_id_col].isin(cl), 'duplicates_cluster'] = i
            df_pairs.loc[df_pairs['duplicate_id'].isin(cl), 'duplicates_cluster'] = i
        df_pairs['duplicates_cluster'] = df_pairs['duplicates_cluster'].astype(int)
        self.clusters = clusters
        self.duples_data = df_pairs

    def extract_pairs(self):
        if 'duplicates' in self._dirty_norm_data.columns:
            df_duples = self._dirty_norm_data[[self.obj_id_col, 'duplicates']].copy()
            df_duples.fillna('', inplace=True)
            duplicates_list = list()
            for item in df_duples[[self.obj_id_col, 'duplicates']].itertuples(index=False):
                item_matches = []
                duples_list = item[1] if type(item[1]) == list else eval(item[1])
                if len(duples_list) > 0:
                    for row in duples_list:  # eval
                        dupl_item = {self.obj_id_col: row['source_id'],
                                     'duplicate_id': row['duplicate_id']}
                        del row['duplicate_id']
                        del row['source_id']
                        dupl_item['duplicates'] = row
                        item_matches.append(dupl_item)
                    if len(item_matches) > 0:
                        duplicates_list += item_matches
            return pd.DataFrame(duplicates_list)[[self.obj_id_col, 'duplicate_id']]
        else:
            raise Exception("Unable to extract duplicates list!")

    @property
    def duples_wide_data(self):
        if self._duples_wide_data is None:
            try:
                self._duples_wide_data = pd.read_csv(self.duples_wide_filename, converters=self.converters)
                self._duples_wide_data.fillna('', inplace=True)
            except:
                pass
        return self._duples_wide_data

    @property
    def duples_long_data(self):
        if self._duples_long_data is None:
            try:
                self._duples_long_data = pd.read_csv(self.duples_long_filename, converters=self.converters)
                self._duples_long_data.fillna('', inplace=True)
            except:
                pass
        return self._duples_long_data

    @property
    def dirty_norm_data(self):
        if self._dirty_norm_data is None:
            if os.path.isfile(self.dirty_norm_filepath) is not True:
                try:
                    df_norm = self.prepare_data()
                    df_norm.to_csv(self.dirty_norm_filepath, index=False)
                except Exception as e:
                    raise Exception("Unable to normalize the dirty dataframe!") from e
            try:
                self._dirty_norm_data = pd.read_csv(self.dirty_norm_filepath, converters=self.converters)
                self._dirty_norm_data.fillna('', inplace=True)
            except Exception as e:
                raise Exception("Unable to read normalized dirty dataframe!") from e
        return self._dirty_norm_data

    @property
    def dirty_data(self):
        if self._dirty_data is None:
            if os.path.isfile(self.dirty_filepath) is not True:
                try:
                    self._dirty_data = self.read_data()
                    self._dirty_data.to_csv(self.dirty_filepath)
                except Exception as e:
                    raise Exception("Unable to import the dirty data!") from e
            try:
                self._dirty_data = pd.read_csv(self.dirty_filepath, converters=self.converters)
                self._dirty_data.fillna('', inplace=True)
            except Exception as e:
                raise Exception("Unable to read the dirty dataframe!") from e
        return self._dirty_data
