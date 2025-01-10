import os
import copy
import re
import sys
import traceback
from pathlib import Path

import haggis.string_util
import numpy as np
import pandas as pd
import seedir
import exso
from exso import Files
from exso.IO.Nodes import Node, Group, DNA
from exso.IO.Search import Search


#***********************************
# Add the len() method to pathlib (get depth of a directory/file, counting from "C:/"
def pathlength(self):
    p = str(self)
    # depth = len(re.split('[\\\]', p))
    depth = len(self.parts)
    return depth

# Add the add() method (+ operator)
# Usage: when you just want to add an extension to a name but you don't know apriori which extension, or just dont want to carry it arround
# (e.g. path = one/two/three --> path = path + '.csv', would crash under normal pathlib behavior. Now it's possible
# It is not used to propel into a directory. For that, there's the builtin "/" method.

def add(self, other):
    p = str(self)
    new = p + other
    return Path(new)

setattr(Path, "__len__", pathlength)
setattr(Path, "__add__", add)


# ********   *********   *********   *********   *********   *********   *********   *********
# ********   *********   *********   *********   *********   *********   *********   *********
# ********   *********   *********   *********   *********   *********   *********   *********
class TreeAccessors:
    # ********   *********   *********   *********   *********   *********   *********   *********
    def dna_match(self, search_for:str|DNA):
        if isinstance(search_for, str):
            search_for = DNA(search_for)
        if search_for.root == 'root':
            return search_for
        else:
            root_node = self.get_nodes_whose('name',search_for.root, collapse_if_single = True)
            if isinstance(root_node, Group):
                raise NameError("The provided tree-argument ('{}') is ambiguous, \nsince it was encountered more than once ({} possible matches) in the tree.\nBe more specific.\n".format(search_for, len(root_node)))

            absolute_dna = root_node.parent.dna + search_for
            return DNA(absolute_dna)
    # ********   *********   *********   *********   *********   *********   *********   *********
    def get_nodes_whose(self, attribute, equals, collapse_if_single = False):

        if attribute == 'path':
            equals = str(Path(equals).absolute())
            target_nodes = [n for n in self.nodes if str(getattr(n, attribute).absolute())==equals]

        elif isinstance(equals, str):
            target_nodes = [n for n in self.nodes if getattr(n, attribute).lower()==equals.lower()]
            target_nodes = [n for n in self.nodes if getattr(n, attribute).lower()==equals.lower()]

        elif isinstance(equals, list):
            target_nodes = []
            target_nodes.extend([n for n in self.nodes for eq in equals if getattr(n,attribute)== eq])

        elif isinstance(equals, DNA):
            target_nodes = [n for n in self.nodes if getattr(n, attribute)==equals]


        else:
            raise IOError("The 'equals' argument must be a string")

        if len(target_nodes) > 1 or collapse_if_single == False:
            return Group(target_nodes)

        elif len(target_nodes)==1:
            return target_nodes[0]

        elif len(target_nodes)==0:
            propose = self.search(equals, n_best = 3)
            raise ValueError("\n\nCould not locate node whose attribute:{} equals: {}. \n\n\tDid you mean any of the options below?\n"
                          "{}".format(attribute, equals, haggis.string_util.align(propose.dna, alignment = 'left', width = 1)))


    # *******  *******   *******   *******   *******   *******   *******
    def get_node(self, locator: None| str | Path | DNA | Node = None):
        node = None
        if isinstance(locator, Path):
            node = self.get_nodes_whose('path', equals=locator, collapse_if_single=True)
        elif isinstance(locator, Node):
            node = locator
        elif isinstance(locator, str):
            if os.path.isfile(locator):
                node = self.get_nodes_whose('path', equals=locator, collapse_if_single=True)
            else:
                if '>>' in locator:
                    explicit_dna = re.search('.*(?=.>>)', locator).group()
                    explicit_node = self.get_node(explicit_dna)
                    node = explicit_node.get_relevants(depth = self.max_depth - 1)[0]
                else:
                    locator = DNA(locator)
                    node = self.get_node(locator)

        elif isinstance(locator,DNA):
            absolute_dna_locator = self.dna_match(locator)
            node = self.get_nodes_whose('dna', equals=absolute_dna_locator, collapse_if_single=True)

        elif isinstance(locator, type(None)):
            node = self.root
        else:
            Warning("Invalid locator passed: {} ({})".format(locator, type(locator)))

        return node
    # ********   *********   *********   *********   *********   *********   *********   *********
    def __getitem__(self, item):
        return self.get_node(locator=item)
    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********


# ********   *********   *********   *********   *********   *********   *********   *********
# ********   *********   *********   *********   *********   *********   *********   *********
# ********   *********   *********   *********   *********   *********   *********   *********
class TreeConstructors:
    # ********   *********   *********   *********   *********   *********   *********   *********
    def make_dna_chains(self):
        self.dna_is_initialized = True

        for n in self.nodes:
            n.make_siblings()
            n.make_children()

        for i in range(len(self.nodes) - 1, -1, -1): # from max to min (min = 0, but -1 to actually reach zero)
            n = self.nodes[i]
            n.make_descendants()

        for i in range(len(self.nodes)):
            n = self.nodes[i]
            n.make_ascendants()
            n.make_dna()

    # ********   *********   *********   *********   *********   *********   *********   *********
    def hot_start(self, root, root_content):
        ''' input is a node, and an arbitrary dictionary, finally leading to dataframe leaves
        '''
        # new_depth = root.depth + 1
        # kind = root.depth2kind(new_depth)
        child_depth = root.depth + 1

        if isinstance(root_content, dict):
            for child_name, child_content in root_content.items():
                child_path = root.path / child_name

                if isinstance(child_content, dict):
                    child_kind = 'dir'
                elif isinstance(child_content, pd.DataFrame):
                    child_kind = 'file'
                    child_path = child_path.with_suffix('.csv') # hot start only has names, not paths. So i manually add it

                # if kind == 'file':
                #     new_root = new_root + '.csv'

                fruit = None
                if isinstance(child_content, pd.DataFrame) and self.ignore_fruits == False:
                    fruit = child_content

                node = Node(child_name, depth = child_depth, path = child_path, kind = child_kind, parent = root, fruit=fruit)
                # parent = Node(k, depth=new_depth, path=new_root, kind=kind, parent=root, fruit=fruit)

                self.nodes.append(node)
                self.hot_start(node, child_content)

        elif isinstance(root_content, pd.DataFrame):

            if isinstance(root_content.index, pd.MultiIndex):
                cols = root_content.columns.levels[0].to_list()
                root.is_multiindex = True
            else:
                cols = root_content.columns

            for c in cols:
                try:
                    property_path = root.path / c
                except:
                    print(root_content.head())
                    print()
                    print('Problematic column:', c)
                    print()
                    sys.exit()

                n = Node(name=c, depth=child_depth, path=property_path, kind='property', parent=root)
                self.nodes.append(n)


    # ********   *********   *********   *********   *********   *********   *********   *********
    def cold_start(self, root, xml_tree):
        ''' By default ignores any file that is not a csv.
            But also ignores the start-pattern specified in the __init__'''
        for path in root.path.iterdir():

            if any(list(map(lambda x: path.name.startswith(x), self.ignore_if_startswith))):
                continue
            depth = root.depth + 1

            if path.is_dir():
                kind = 'dir'
                xml_tree[path.name] = {}
                parent = Node(path.name, depth=depth, path=path, kind=kind, parent=root)
                self.nodes.append(parent)
                xml_tree[path.name].update(self.cold_start(parent, xml_tree[path.name]))

            elif path.is_file(): # iterdir() automatically includes the .csv extension (on the contrrry of hot-start mode)

                if path.suffix != '.csv':
                    continue

                kind = 'file'
                entity_name = path.with_suffix("").name
                xml_tree[entity_name] = []
                f = Node(name=entity_name, depth=depth, path=path, kind=kind, parent=root)
                self.nodes.append(f)

                if 'Unavailability_Reason' in path.name:
                    f.encoding = 'utf-16'
                else:
                    f.encoding = 'utf-8'

                try:
                    cols = pd.read_csv(path, nrows=1, index_col=0, encoding=f.encoding, sep= exso._list_sep, decimal=exso._decimal_sep).columns.to_list()
                except:
                    print()
                    print("ERROR while sniffing file:", path)
                    print()
                    print(traceback.format_exc())
                    sys.exit()

                is_multiindex = any([c.startswith('Unnamed') for c in cols])

                if is_multiindex:
                    f.is_multiindex = True
                    cols = Node.read_multiindex(object, path, nrows=1).columns.levels[0]

                for c in cols:
                    property_path = path / c
                    n = Node(name=c, depth=depth + 1, path=property_path, kind='property', parent=f)
                    self.nodes.append(n)
                    xml_tree[entity_name].append(c)

        return xml_tree


# ********   *********   *********   *********   *********   *********   *********   *********
# ********   *********   *********   *********   *********   *********   *********   *********
# ********   *********   *********   *********   *********   *********   *********   *********
class Tree(Search, TreeConstructors, TreeAccessors):
    ''' Tree Class: a custom database framework for nested csv files

        :param root_path: [str|Path|None] [default = None] The path of the database directory
        :param root_dict: [dict|None] [default = None] If provided, a hot-start tree will be created. Format: structured or unstructured dict, whose final leafs are DataFr
        :param depth_mapping (optional): [dict|None]. A mapping, of k:v pairs. Keys are the depth (relative to the root, starting from 0) and values are strings. e.g. {0:'root', 1:'publisher', ...}
        :param root_name (optional): [str] [default = 'root']. A custom string to name the root node. default = 'root'
        :param ignore_initials (optional): [str|list] [default = '.']. Pattern(s) to ignore if encountered on the beggining of any file or directory in the tree.
        :param ignore_fruits (optional): [bool] [default = False]

        Usage: - Instantiate a Tree object
               - use the indicated accessors (see documentation) to access, transform, combine, plot and export any node(s)
    '''
    instances = []
    def __init__(self, root_path:Path|str|None = None, root_dict:dict|None = None, depth_mapping:dict|None = None, ignore_if_startswith:str|list = '.', root_name:str|None = None, make = True, ignore_fruits = False):

        self.instances.append(self)

        self.compile_input(root_path, root_dict, depth_mapping, ignore_if_startswith, root_name, ignore_fruits)

        self.root = Node(name=self.root_name, depth=0, path=self.root_path, kind='dir', parent=None)
        self.nodes = [self.root]

        if make:
            if self.start_mode == 'hot':
                self.hot_start(root = self.root, root_content=root_dict)
            else:
                self.cold_start(root = self.root, xml_tree={})

            self.max_depth = sorted([n.depth for n in self.nodes])[-1]
            self.size = len(self.nodes)
            self.nodes = Group(self.nodes)
            self.make_dna_chains()

            if self.depth_mapping:
                self.assign_depth_names()

    # ********   *********   *********   *********   *********   *********   *********   *********
    def compile_input(self, root_path, root_dict, depth_mapping, ignore_if_startswith, root_name, ignore_fruits):

        # make sure root_path is Path
        if isinstance(root_path, str):
            root_path = Path(root_path)

        elif isinstance(root_path, type(None)) and isinstance(root_dict, type(None)):
            # now, assume defalt database path
            root_path = exso.fp_default_database
        elif isinstance(root_path, type(None)):
            assert isinstance(root_dict, dict)

        if isinstance(root_path, Path):
            start_depth = len(root_path)
            if isinstance(root_dict, dict):
                start_mode = 'hot'
            else:
                start_mode = 'cold'
                if not root_path.exists():
                    raise AssertionError("The DataBase path provided ('{}') does not exist".format(root_path))
        else:
            start_mode = 'hot'
            assert isinstance(root_dict, dict)
            start_depth = 0

        if isinstance(ignore_if_startswith, str):
            ignore_if_startswith = [ignore_if_startswith]

        if not root_name:
            root_name = "root"

        if depth_mapping:
            # this means, there is a strict connection between depth and kind.
            # so, if:    root > sth > sthelse   is a directory (of a custom-kind = report)
            # then also: root > sth2 > sthelse2 is a directory (of a custom-kind = report)
            assert isinstance(depth_mapping, dict)
        else:
            # no strict relation between depth and kind
            # kinds will be given as root / dir / file / property (column)
            depth_mapping = {}

        self.root_path = root_path
        self.root_dict = root_dict
        self.start_mode = start_mode
        self.ignore_if_startswith = ignore_if_startswith
        self.root_name = root_name
        self.start_depth = start_depth
        self.ignore_fruits = ignore_fruits

        self.dna_is_initialized = False
        self.depth_mapping = depth_mapping

    # ********   *********   *********   *********   *********   *********   *********   *********
    # def calibrate_depths(self, zero_depth_kind):
    #
    #     kinds = ['root', 'publisher', 'report', 'field', 'file', 'property']
    #     reference_depth = [i for i, k in enumerate(kinds) if k == zero_depth_kind][0]
    #     calibrated_kinds = kinds[reference_depth:]
    #
    #     kinds = {i: k for i, k in enumerate(calibrated_kinds)}
    #     self.kinds = kinds

    # ********   *********   *********   *********   *********   *********   *********   *********
    def assign_depth_names(self):
        assert self.max_depth <= max([k for k in self.depth_mapping.keys()])
        for n in self.nodes:
            n.kind = self.depth_mapping[n.depth]

    # ********   *********   *********   *********   *********   *********   *********   *********
    def make_tree_bak(self, from_dict=None, ignore_fruits = True):

        if from_dict:
            self.ignore_fruits = ignore_fruits
            tree_dict = from_dict
            self.hot_start(self.root, tree_dict)

        else:
            tree_dict = self.cold_start(self.root, {})
            with open(Files._exso_dir / 'tree.txt', 'w', encoding='utf-8') as f:
                f.write(str(tree_dict))

        self.tree_dict = tree_dict
        self.nodes = Group(self.nodes)
        self.size = len(self.nodes)
        self.max_depth = sorted([n.depth for n in self.nodes])[-1]

        self.make_dna_chains()
        return self

    # ********   *********   *********   *********   *********   *********   *********   *********
    def make_dirs(self):
        field_nodes = self.get_nodes_whose('kind', equals='field')
        # self.delete_if_empty(field_nodes)
        [fn.path.mkdir(exist_ok=True, parents=True) for fn in field_nodes]

    # ********   *********   *********   *********   *********   *********   *********   *********
    def is_empty(self, path:Path|None = None):
        if isinstance(path, type(None)):
            path = self.root_path
        if not path.exists():
            return True
        if path.is_file():
            return False
        if path.is_dir():
            gen = path.iterdir()
            try:
                next(gen)
                return False
            except:
                return True

    # ********   *********   *********   *********   *********   *********   *********   *********
    def delete_if_empty(self, field_nodes):
        for f in field_nodes:
            if f.path.exists():
                if self.is_empty(f.path):
                    f.path.rmdir()
        input('In Database.Structure, line 170, method "delete_if_empty"')

    # ********   *********   *********   *********   *********   *********   *********   *********
    def visualize(self):
        # STR.tree(self.root.path, level = 2)
        seedir.seedir(self.root.path, sticky_formatter=True, style='emoji')

    # ********   *********   *********   *********   *********   *********   *********   *********
    def combine(self, *locators, with_name:str|None = None, handle_synonymity:str|list = 'auto', resolution = 'auto'):
        nodes = list(map(self.get_node, *locators))

        if handle_synonymity == 'auto':
            suffixes = ["_" + str(i) for i in range(1, len(*locators)+1,1)]
        else:
            suffixes = handle_synonymity

        dfs = [node() for node in nodes]
        original_cols = [df.columns.to_list() for df in dfs].copy()

        # if a column is found in more than one dataframe columns, add a suffix to distinguish it

        new_cols = copy.deepcopy(original_cols)
        for i in range(len(original_cols)):  # for each list of df columns
            examined_cols = original_cols[i]  # we look at the originals
            visited = []
            for j in range(len(original_cols)):
                if j == i:  # dont compare to yourself
                    continue

                other = original_cols[j]  # we look at the originals of other dataframes
                conflicts = np.intersect1d(np.array(examined_cols), np.array(other))  # are there any conflicts at all?

                if len(conflicts):  # if yes,
                    for conf in conflicts:  # for each conflictual string:
                        examined_arg = [i for i, col in enumerate(examined_cols) if
                                        col == conf and i not in visited]  # find its location in the examined list (only if it hasnt been revisited)
                        if not examined_arg:
                            continue
                        else:
                            examined_arg = examined_arg[0]

                        visited.append(examined_arg)
                        new_cols[i][examined_arg] += suffixes[i]  # and rename the new columns accordingly

        renamers = [{k: v for k, v in zip(original_cols[i], new_cols[i])} for i in range(len(new_cols))]  # create one renamer per dataframe

        [dfs[i].rename(renamers[i], axis = 'columns', inplace = True) for i in range(len(dfs))]

        resolutions = [dfs[i].index[1] - dfs[i].index[0] for i in range(len(dfs))]
        if resolution == 'auto' or resolution == 'min':
            selected_resolution = min(resolutions)
            for i in range(len(resolutions)):
                if resolutions[i] != selected_resolution:
                    dfs[i] = dfs[i].resample(selected_resolution).interpolate(method='linear')

        elif resolution == 'max':
            selected_resolution = max(resolutions)
            for i in range(len(resolutions)):
                if resolutions[i] != selected_resolution:
                    dfs[i] = dfs[i].resample(selected_resolution).mean() #

        else:
            selected_resolution = resolution
            for i in range(len(resolutions)):
                if resolutions[i] != selected_resolution:
                    dfs[i] = dfs[i].resample(selected_resolution).mean()  #


        df = pd.concat([*dfs], axis = 1)
        if not with_name:
            with_name = "_".join(Group(nodes).name)


        node = Node(with_name, path = self.root.path / ".virtual" / with_name, depth = 2, kind = 'file', parent = self.root, fruit = df)

        return node
    # ********   *********   *********   *********   *********   *********   *********   *********





