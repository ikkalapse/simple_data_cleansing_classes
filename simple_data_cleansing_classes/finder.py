import pandas as pd
import os
import re

try:
    from .data import Data
    from .project import Project
except:
    from simple_data_cleansing_classes.data import Data
    from simple_data_cleansing_classes.project import Project


class Finder:
    project = None  # Project where to store the data

    data = None

    matches_column = 'matches'  # Column name for matches data

    _matches = None

    def __init__(self, project: Project, *args, **kwargs):

        self.project = project
        self.data = args  # dataframes into list
        self.matches_column = kwargs.get('matches_column', self.matches_column)
        # Files
        self.matches_filename = self.matches_column + ".csv"
        self.matches_pairwise_filename = "_".join([self.matches_column, "pairwise"]) + ".csv"
        # self.matches_wide_filename = "_".join([self.matches_column, "wide.csv"])
        # self.matches_long_filename = "_".join([self.matches_column, "long.csv"])
        # Initial values of class variables
        self._matches = None  # Working dict for saving search results
        self._matches_data = None  # Matches dataframe
        self._matches_pairwise_data = None  # Matches pairwise dataframe
        # self._matches_wide_data = None  # Matches data in wide format
        # self._matches_long_data = None  # Matches data in long format

    def process(self):
        """Searching, clustering and saving duplicates."""

        self.search_matches()
        self.save_matches()

    def search_matches(self):
        """Method for filling self.matches dictionary."""

        raise NotImplementedError

    def save_matches(self):
        """Saving matches self._matches_data to a CSV-file self.matches_filename."""

        self._matches_data = pd.DataFrame(self._matches)
        self._matches_data.to_csv(self.matches_filename, index=False)
        self._matches_pairwise_data = pd.DataFrame(self._extract_matches_pairwise())[['source_id', 'target_id']]
        self._matches_pairwise_data.to_csv(self.matches_pairwise_filename, index=False)

    def _extract_matches_pairwise(self):
        """Extracting pairs source_id -- target_id (many-to-many pairwise)."""

        matches_list = list()
        for item in self._matches_data[[self.data_1.id_column, self.matches_column]].itertuples(index=False):
            item_matches = []
            matches_list = item[1] if type(item[1]) == list else eval(item[1])
            if len(matches_list) > 0:
                for row in matches_list:  # eval
                    match_item = {'source_id': row['source_id'], 'target_id': row['target_id']}
                    del row['target_id']
                    del row['source_id']
                    match_item['matches'] = row
                    item_matches.append(match_item)
                if len(item_matches) > 0:
                    matches_list += item_matches
        return matches_list

    @property
    def matches_data(self):
        self._matches_data = None
        if os.path.isfile(self.matches_filename):
            try:
                self._matches_data = pd.read_csv(self.matches_filename,
                                                 converters=self.converters)
                self._matches_data.fillna('', inplace=True)
            except Exception as e:
                raise Exception("Unable to read matches data!") from e
        return self._matches_data

    @property
    def matches_pairwise_data(self):
        self._matches_pairwise_data = None
        if os.path.isfile(self.matches_pairwise_filename):
            try:
                self._matches_pairwise_data = pd.read_csv(self.matches_pairwise_filename,
                                                          converters=self.converters)
                self._matches_pairwise_data.fillna('', inplace=True)
            except Exception as e:
                raise Exception("Unable to read pairwise matches data!") from e
        return self._matches_pairwise_data

    @property
    def converters(self):
        """All converters for all data in one dictionary."""

        converters = {}
        for item in self.data:
            converters.update(item.converters)
        return converters

    @property
    def matches_wide_data(self):
        if self._matches_wide_data is None:
            try:
                self._matches_wide_data = pd.read_csv(self.matches_wide_filename,
                                                      converters=self.converters)
                self._matches_wide_data.fillna('', inplace=True)
            except:
                pass
        return self._matches_wide_data

    @property
    def matches_long_data(self):
        if self._matches_long_data is None:
            try:
                self._matches_long_data = pd.read_csv(self.matches_long_filename,
                                                      converters=self.converters)
                self._matches_long_data.fillna('', inplace=True)
            except:
                pass
        return self._matches_long_data

    def make_wide(self):
        """Creating and saving wide-dataframe with merged columns from both data."""

        try:
            self._matches_wide_data = pd.merge(self.matches_pairwise_data,
                                               self.data_1.data,
                                               left_on='source_id',
                                               right_on=self.data_1.id_column)
            _data_2 = self.data_2 if self.data_2 is not None else self.data_1
            self._matches_wide_data = pd.merge(self.matches_pairwise_data,
                                               _data_2.data,
                                               left_on='target_id', right_on=_data_2.id_column,
                                               suffixes=('-source', '-target')) \
                .rename(columns={self.data_1.id_column + '-source': self.data_1.id_column}) \
                .drop([self.data_1.id_column + '-target'], axis=1)
            self._duples_wide_data.to_csv(os.path.join(self.project.matches_dir, self.duples_wide_filename))
        except Exception as e:
            raise Exception("Unable to create wide dataframe with matches!") from e

    def make_long(self):

        try:
            df_wide = self.matches_wide_data
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
            self._matches_long_data = df_long
            self._matches_long_data.to_csv(os.path.join(self.project.matches_dir, self.duples_long_filename))
        except Exception as e:
            raise Exception("Unable to create long dataframe with duplicates") from e

    def __getattr__(self, name):
        if name not in self.__dict__ and 'data_' in name:
            ind = re.findall(r'data\_([0-9]+)', name)
            try:
                self.__dict__[name] = self.data[int(ind[0]) + 1]
            except:
                self.__dict__[name] = None
        return self.__dict__[name]


if __name__ == '__main__':
    project = Project(os.path.join("..", "projects", "__test_finder_class__"))

    finder = Finder(project)

    print(finder.data_1)
