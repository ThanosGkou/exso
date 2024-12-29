import os
import re


# ********   *    ********   *    ********   *    ********   *   ********
# ********   *    ********   *    ********   *    ********   *   ********
# ********   *    ********   *    ********   *    ********   *   ********
class Paths:
    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def make_glob_filter(scan_dir, eligibility:dict, scan_for_dirs = False):

        '''
        :param scan_dir: the path to the directory in which to search for files
        :param eligibility: dictionary: {'start_filter':fdf,
                                         'end_filter':dfdf,
                                         'inbetween_filter':safa,
                                         'extension_filter':dfd}

        :return: the rule (can be used directly in glob.glob(pathname = rule)
        '''

        keys_superset = ['start_filter', 'end_filter', 'inbetween_filter', 'extension_filter']
        for k in keys_superset:
            if k not in eligibility.keys():
                eligibility[k] = '*'

        if scan_for_dirs:
            rule = r'{}\{}*{}*{}'.format(scan_dir,
                                            eligibility['start_filter'],
                                            eligibility['inbetween_filter'],
                                            eligibility['end_filter'])
        else:
            rule = r'{}\{}*{}*{}.{}'.format(scan_dir,
                                            eligibility['start_filter'],
                                            eligibility['inbetween_filter'],
                                            eligibility['end_filter'],
                                            eligibility['extension_filter'])

        rule = re.sub(r'\*\*+', '*', rule)  # replace multi conseq. stars

        return rule


    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def listdir(dir, only_directories = True):

        files = os.listdir(dir)
        valid = []
        for f in files:
            if os.path.isdir(os.path.join(dir,f)):
                valid.append(f)

            elif os.path.isfile(os.path.join(dir, f)):
                if only_directories:
                    pass
                else:
                    if f[0] != "~":
                        valid.append(f)
        return valid

    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def get_file_leaves(dir):

        names = []
        filepaths = []
        walker = os.walk(dir)
        for f in walker:
            root, dirs, files = f
            if files:
                for file in files:
                    name = file.split('.')[0]
                    filepath = os.path.join(root, file)
                    names.append(name)
                    filepaths.append(filepath)

        return names, filepaths

    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def get_tree(root_dir):
        ''' this will dig the directory to find ANYTHING of use.
                   but if it founds a directory, AND all its subdirs are empty (no files), it will return empty clause for that branch
       '''
        names = []
        filepaths_tree = {}
        walker = os.walk(root_dir)
        for f in walker:
            root, dirs, files = f
            if files:
                root_name = os.path.split(root)[-1]
                filepaths_tree[root_name] = {}
                for file in files:
                    name = file.split('.')[0]
                    filepath = os.path.join(root, file)
                    names.append(name)
                    filepaths_tree[root_name][name] = filepath
            else:
                pass

        return filepaths_tree

    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********

