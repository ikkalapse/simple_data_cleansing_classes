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


class FindRelations(Finder):

    data_first = None  # First dataframe

    data_second = None  # Second dataframe

    project = None  # Project where to store duplicates

    # result_cols = None  # List of columns in result dataframe

    def __init__(self, project: Project, data_first: Data, data_second: Data, **kwargs):

        Finder.__init__(self, project, **kwargs)
        self.data_first = data_first
        self.data_second = data_second

    def process(self):
        """Searching, clustering and saving duplicates."""

        self.process()
        self.make_wide()
        self.make_long()

    def search_matches(self):
        raise NotImplementedError
