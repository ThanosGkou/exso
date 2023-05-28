import os
import re
import sys
import traceback
from pathlib import Path

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
    depth = len(re.split('[\\\]', p))
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

        if isinstance(equals, str) or isinstance(equals, Path):
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

            raise IOError("Could not locate node whose attribute:{} equals: {}".format(attribute, equals))

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
    def hot_start(self, root, content):
        new_depth = root.depth + 1
        kind = root.depth2kind(new_depth)
        if isinstance(content, dict):
            for k, v in content.items():
                new_root = root.path / k
                if kind == 'file':
                    new_root = new_root + '.csv'

                if isinstance(v, pd.DataFrame):
                    if self.ignore_fruits:
                        fruit = None
                    else:
                        fruit = v
                else:
                    fruit = None

                parent = Node(k, depth=new_depth, path=new_root, kind=kind, parent=root, fruit=fruit)
                self.nodes.append(parent)
                self.hot_start(parent, v)

        elif isinstance(content, pd.DataFrame):

            if isinstance(content.index, pd.MultiIndex):
                cols = content.columns.levels[0].to_list()
                root.is_multiindex = True
            else:

                cols = content.columns

            for c in cols:
                try:
                    property_path = root.path / c
                except:
                    print(content.head())
                    print()
                    print('Problematic column:', c)
                    print()
                    sys.exit()

                nn = Node(name=c, depth=new_depth, path=property_path, kind=root.depth2kind(new_depth),
                          parent=root)
                self.nodes.append(nn)

        elif isinstance(content, list):
            for k in content:

                n = Node(k, depth=new_depth, path=root.path / (k+'.csv'), kind=kind, parent=root)
                self.nodes.append(n)
                if n.path.exists():
                    if 'Unavailability_Reason' in n.path.name:
                        n.encoding = 'utf-16'
                    else:
                        n.encoding = 'utf-8'

                    cols = pd.read_csv(n.path, nrows=1, index_col=0, encoding=n.encoding, sep=exso._list_sep, decimal=exso._decimal_sep).columns.to_list()
                    is_multiindex = any([c.startswith('Unnamed') for c in cols])
                    if is_multiindex:
                        n.is_multiindex = True

                    for c in cols:
                        property_path = n.path / c
                        nn = Node(name=c, depth=new_depth + 1, path=property_path, kind=root.depth2kind(new_depth + 1), parent=n)
                        self.nodes.append(nn)

    # ********   *********   *********   *********   *********   *********   *********   *********
    def cold_start(self, root, xml_tree):
        ''' By default ignores any file that is not a csv.
            But also ignores the start-pattern specified in the __init__'''
        for path in root.path.iterdir():
            if any(list(map(lambda x: path.name.startswith(x), self.ignore_initials))):
                continue
            depth = root.depth + 1
            kind = root.depth2kind(depth)

            if path.is_dir():
                xml_tree[path.name] = {}
                parent = Node(path.name, depth=depth, path=path, kind=kind, parent=root)
                self.nodes.append(parent)
                xml_tree[path.name].update(self.cold_start(parent, xml_tree[path.name]))
            else:
                if 'csv' in path.name:
                    entity_name = path.name.split('.')[0]
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
                        n = Node(name=c, depth=depth + 1, path=property_path, kind=root.depth2kind(depth + 1), parent=f)
                        self.nodes.append(n)
                        xml_tree[entity_name].append(c)

        return xml_tree


# ********   *********   *********   *********   *********   *********   *********   *********
# ********   *********   *********   *********   *********   *********   *********   *********
# ********   *********   *********   *********   *********   *********   *********   *********
class Tree(Search, TreeConstructors, TreeAccessors):
    ''' Tree Class: a custom database framework for nested csv files

        :param root_path: [str|Path] The path of the database directory
        :param zero_depth_kind (optional): [str]. Any of: root, publisher, report, field, file. default = 'root'
        :param zero_depth_name (optional): [str]. A custom string to name the root node. default = 'root'
        :param ignore_inintials (optional): [str|list]. Pattern(s) to ignore if encountered on the beggining of any file or directory in the tree. default = '.'

        Usage: - Instantiate a Tree object
               - call the .make_tree() method
               - use the indicated accessors (see documentation) to acess, transform, plot and export any node(s)
    '''
    instances = []
    def __init__(self, root_path, zero_depth_kind='root', zero_depth_name='root', ignore_initials:str|list = '.'):
        self.instances.append(self)
        if isinstance(ignore_initials, str):
            ignore_initials = [ignore_initials]
        if isinstance(root_path, str):
            root_path = Path(root_path)
        self.ignore_initials = ignore_initials
        self.init_depth = len(root_path)
        self.dna_is_initialized = False
        self.calibrate_depths(zero_depth_kind)
        self.root = Node(name=zero_depth_name, depth=0, path=root_path, kind=zero_depth_kind, parent=None)

        self.nodes = [self.root]

    # ********   *********   *********   *********   *********   *********   *********   *********
    def calibrate_depths(self, zero_depth_kind):

        kinds = ['root', 'publisher', 'report', 'field', 'file', 'property']
        reference_depth = [i for i, k in enumerate(kinds) if k == zero_depth_kind][0]
        calibrated_kinds = kinds[reference_depth:]

        kinds = {i: k for i, k in enumerate(calibrated_kinds)}
        self.kinds = kinds

    # ********   *********   *********   *********   *********   *********   *********   *********
    def make_tree(self, from_dict=None, ignore_fruits = True):

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

    # ********   *********   *********   *********   *********   *********   *********   *********
    def make_dirs(self):
        field_nodes = self.get_nodes_whose('kind', equals='field')
        # self.delete_if_empty(field_nodes)
        [fn.path.mkdir(exist_ok=True, parents=True) for fn in field_nodes]

    # ********   *********   *********   *********   *********   *********   *********   *********
    def is_empty(self, path:Path):
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
    def combine(self, *locators, tz_pipe = ['utc', 'eet', None], start_date = None, end_date = None):
        nodes = list(map(self.get_node, *locators))
        dfs = [node(start_date = start_date, end_date = end_date) for node in nodes]
        cols = []
        for df in dfs:
            cols.extend(df.columns.to_list())
        df = pd.concat([*dfs], axis = 1)
        if tz_pipe:
            df = df.tz_localize(tz_pipe[0]).tz_convert(tz_pipe[1])
            if len(tz_pipe) == 3:
                df = df.tz_localize(tz_pipe[2])
        return df





