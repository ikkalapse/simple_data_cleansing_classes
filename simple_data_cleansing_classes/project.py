import os


class Project:

    project_dir = None  # Project directory path

    sub_dirs = None   # Subdirectories included into the project directory

    def __init__(self, project_dir, sub_dirs=None):
        self.project_dir = os.path.abspath(project_dir)  # Project directory
        os.makedirs(self.project_dir, exist_ok=True)
        try:
            self.sub_dirs = {}
            for item in sub_dirs:
                self.sub_dirs[item] = os.path.abspath(os.path.join(self.project_dir, item))
                os.makedirs(self.sub_dirs[item], exist_ok=True)
        except Exception as e:
            raise Exception("Unable to create subdirectory!", str(e))

    def __getattr__(self, name):
        if name not in self.__dict__:
            dir_name = name[:-len("_dir")]
            if dir_name in self.sub_dirs and name[-len("_dir"):] == '_dir':
                self.__dict__[name] = self.sub_dirs[dir_name]
        return self.__dict__[name]


if __name__ == '__main__':

    test_project = Project(os.path.join("..", "projects", "test"), ['datasets', 'tests'])
    print(test_project.project_dir)
    print(test_project.datasets_dir)
    print(test_project.tests_dir)
