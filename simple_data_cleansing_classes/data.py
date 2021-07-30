import os
import pandas as pd
from simple_data_cleansing_classes.project import Project


class Data:

    project = None  # Project

    obj_id_col = None  # Name of column with ID of objects

    prefix = None

    converters = None  # Converters for reading data using Pandas

    def __init__(self, project: Project, obj_id_col, prefix='', **kwargs):
        # Project name
        self.project = project
        self.obj_id_col = obj_id_col
        self.prefix = prefix
        self.converters = kwargs.get('converters', self.converters)
        # Files
        self.data_filename = "_".join([self.prefix, "data.csv"])
        self.data_norm_filename = "_".join([self.prefix, "data_norm.csv"])
        self.data_filepath = os.path.join(self.project.datasets_dir, self.data_filename)
        self.data_norm_filepath = os.path.join(self.project.datasets_dir, self.data_norm_filename)
        # Initial values of class variables
        self._data = None  # Dirty data
        self._data_norm = None  # Normalized dirty data

    def prepare_data(self):
        raise NotImplementedError

    def read_data(self):
        raise NotImplementedError

    @property
    def data(self):
        if self._data is None:
            if os.path.isfile(self.data_filepath) is not True:
                try:
                    self._data = self.read_data()
                    self._data.to_csv(self.data_filepath)
                except Exception as e:
                    raise Exception("Unable to import the data!") from e
            try:
                self._data = pd.read_csv(self.data_filepath, converters=self.converters)
                self._data.fillna('', inplace=True)
            except Exception as e:
                raise Exception("Unable to read the data!") from e
        return self._data

    @property
    def data_norm(self):
        if self._data_norm is None:
            if os.path.isfile(self.data_norm_filepath) is not True:
                try:
                    df_norm = self.prepare_data()
                    df_norm.to_csv(self.data_norm_filepath, index=False)
                except Exception as e:
                    raise Exception("Unable to normalize the data!") from e
            try:
                self._data_norm = pd.read_csv(self.data_norm_filepath, converters=self.converters)
                self._data_norm.fillna('', inplace=True)
            except Exception as e:
                raise Exception("Unable to read normalized data!") from e
        return self._data_norm
