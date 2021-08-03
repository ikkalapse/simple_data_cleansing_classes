import pandas as pd
import os
import re

from .data import Data
from .project import Project


class Finder:
    project = None  # Project where to store the data

    data = None  # The data list where to find matches

    matches_column = 'matches'  # Column name for matches

    def __init__(self, project: Project, *args, **kwargs):

        self.project = project
        self.data = list(args)  # dataframes into list
        self.matches_column = kwargs.get('matches_column', self.matches_column)
        # Files
        self.matches_filename = os.path.join(project.project_dir,
                                             self.matches_column + ".csv")
        self.matches_pairwise_filename = os.path.join(project.project_dir,
                                                      "_".join([self.matches_column, "pairwise"]) + ".csv")
        # Initial values of class variables
        self._matches = None  # Working dict for saving search results
        self._df_matches = None  # Matches dataframe
        self._df_matches_pairwise = None  # Matches pairwise dataframe

    def process(self):
        """Searching and saving matches."""

        self.search_matches()
        self.save_matches()

    def search_matches(self):
        """Method for filling self.matches dictionary."""

        raise NotImplementedError

    def save_matches(self):
        """Saving matches self._matches_data to a CSV-file self.matches_filename."""

        self._df_matches = pd.DataFrame(self._matches)
        self._df_matches.to_csv(self.matches_filename, index=False)
        self._df_matches_pairwise = pd.DataFrame(self._extract_matches_pairwise())
        self._df_matches_pairwise.to_csv(self.matches_pairwise_filename, index=False)

    def _extract_matches_pairwise(self):
        """
        Extracts pairs of matches and returns a list of dictionaries.
        """

        matches_list = list()
        for item in self._df_matches[[self.matches_column]].itertuples(index=False):
            # matches list for current item
            item_matches = []
            matches_list = item[0] if type(item[0]) == list else eval(item[0])
            # if matches column contains a list of matches
            if len(matches_list) > 0:
                # get every row in matches
                for row in matches_list:
                    item_matches.append({'source_id': row['source_id'],
                                         'target_id': row['target_id'],
                                         'info': row.get('info', None)})
                if len(item_matches) > 0:
                    matches_list += item_matches
        return matches_list

    @property
    def df_matches(self):
        """Return pandas dataframe contains matches."""

        if self._df_matches is None and os.path.isfile(self.matches_filename):
            try:
                self._df_matches = pd.read_csv(self.matches_filename,
                                               converters=self.converters)
                self._df_matches.fillna('', inplace=True)
            except Exception as e:
                raise Exception("Unable to read matches data!") from e
        return self._df_matches

    @property
    def df_matches_pairwise(self):
        """Return pandas dataframe contains pairwise matches."""

        if self._df_matches_pairwise is None and os.path.isfile(self.matches_pairwise_filename):
            try:
                self._df_matches_pairwise = pd.read_csv(self.matches_pairwise_filename,
                                                        converters=self.converters)
                self._df_matches_pairwise.fillna('', inplace=True)
            except Exception as e:
                raise Exception("Unable to read pairwise matches data!") from e
        return self._df_matches_pairwise

    @property
    def converters(self):
        """All converters for all data in one dictionary."""

        converters = {}
        for item in self.data:
            converters.update(item.converters)
        return converters

    def __getattr__(self, name):
        if name not in self.__dict__ and 'data_' in name:
            ind = re.findall(r'data_([0-9]+)', name)
            try:
                self.__dict__[name] = self.data[int(ind[0]) - 1]
            except:
                self.__dict__[name] = None
        return self.__dict__[name]

    def __setattr__(self, name, value):
        if 'data_' in name:
            ind = re.findall(r'^data_([0-9]+)$', name)
            try:
                self.__dict__['data'][int(ind[0]) - 1] = value
            except IndexError:
                self.__dict__['data'].append(value)
            print(self.__dict__['data'])
        else:
            self.__dict__[name] = value
