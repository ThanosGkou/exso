import inspect
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import exso
from exso.Utils.Plot import Plot


# ********   *********   *********   *********   *********   *********   *********   *********
# ********   *********   *********   *********   *********   *********   *********   *********
# ********   *********   *********   *********   *********   *********   *********   *********
class DNA:
    def __init__(self, dna):
        if isinstance(dna,DNA):
            dna = dna.chain
        else:
            dna = dna.lower()
        self.chain = dna
        self.root = self.split()[0]

    # ********   *********   *********   *********
    def lower(self):
        return self.chain.lower()

    # ********   *********   *********   *********
    def split(self):
        if '.' in self.chain:
            return self.chain.split('.')
        else:
            return [self.chain]
    # ********   *********   *********   *********
    def mkdna(self, other):
        if isinstance(other, DNA):
            return other
        elif isinstance(other, str):
            return DNA(other)
    # ********   *********   *********   *********
    def __str__(self):
        return self.chain
    # ********   *********   *********   *********
    def __repr__(self):
        return self.chain
    # ********   *********   *********   *********
    def __add__(self, other:str):
        other = self.mkdna(other)
        return self.chain + '.' + other.chain
    # ********   *********   *********   *********
    def __lt__(self, other):
        other = self.mkdna(other)
        return str(self.chain) < str(other.chain)
    # ********   *********   *********   *********
    def __gt__(self, other):
        other = self.mkdna(other)
        return str(self.chain) > str(other.chain)
    # ********   *********   *********   *********
    def __eq__(self, other):
        other = self.mkdna(other)
        return str(self.chain) == str(other.chain)
    # ********   *********   *********   *********
    def __ge__(self, other):
        other = self.mkdna(other)
        return self > other or self == other
    # ********   *********   *********   *********
    def __le__(self, other):
        other = self.mkdna(other)
        return self < other or self == other


# ********   *********   *********   *********   *********   *********   *********   *********
# ********   *********   *********   *********   *********   *********   *********   *********
# ********   *********   *********   *********   *********   *********   *********   *********
class NodeAccessors:
    # ********   *********   *********   *********   *********   *********   *********   *********
    def get_list_chain(self):
        if self.kind == 'property':
            raise AssertionError
        chain = self.dna.split[1:]
        return chain

    # ********   *********   *********   *********   *********   *********   *********   *********
    def __str__(self):
        return '"' + str(self.name) + '"'

    # ********   *********   *********   *********   *********   *********   *********   *********
    def __getitem__(self, item):
        if isinstance(item, str):
            return [child for child in self.children if child.name == item][0]
        elif isinstance(item, int):
            return self.children[item]
    # ********   *********   *********   *********   *********   *********   *********   *********
    def get_fruit(self):
        return self()
    # ********   *********   *********   *********   *********   *********   *********   *********
    def get_relevants(self, kind = None, depth = None):
        if kind:
            depth = self.kind2depth(kind)

        if depth > self.depth:
            relevants = [d for d in self.descendants if d.depth == depth]
        elif depth == self.depth:
            relevants = [s for s in self.siblings]
        elif depth < self.depth:
            relevants = [a for a in self.ascendants if a.depth == depth]
        return Group(relevants)

    # ********   *********   *********   *********   *********   *********   *********   *********
    def get_ascendants_of_depth(self, depth):
        ascendants_at_depth = Group([a for a in self.ascendants if a.depth == depth])
        return ascendants_at_depth

    # ********   *********   *********   *********   *********   *********   *********   *********
    def truncate(self, to: str):
        depth = self.kind2depth(to)
        if self.depth < depth:
            return None

        elif self.depth == depth:
            return self

        else:
            ascendants = self.ascendants
            ascendants = [a for a in ascendants if a.depth == depth]
            if len(ascendants) > 1:
                raise Warning("Something wrong. Ascendant should be of length 1.")

            return ascendants[0]
    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********

# ********   *********   *********   *********   *********   *********   *********   *********
# ********   *********   *********   *********   *********   *********   *********   *********
# ********   *********   *********   *********   *********   *********   *********   *********
class NodeConstructors:
    # ********   *********   *********   *********   *********   *********   *********   *********
    def make_siblings(self):
        siblings = []
        for n in self.instances:
            if n.parent == self.parent:
                siblings.append(n)

        self.siblings = Group(siblings)

    # ********   *********   *********   *********   *********   *********   *********   *********
    def make_children(self):
        children = []
        for n in self.instances:
            if n.parent == self:
                children.append(n)
        self.children = Group(children)

    # ********   *********   *********   *********   *********   *********   *********   *********
    def make_descendants(self):
        descendants = []
        for c in self.children:
            descendants.append(c)
            descendants.extend(c.descendants)

        self.descendants = Group(descendants)

    # ********   *********   *********   *********   *********   *********   *********   *********
    def make_ascendants(self):
        if self.parent is None:
            ascendants = []
        else:
            ascendants = [self.parent]
            ascendants.extend(self.parent.ascendants)

        self.ascendants = Group(ascendants)

    # ********   *********   *********   *********   *********   *********   *********   *********
    def make_dna(self):

        dna = ""
        for a in self.ascendants[::-1]:
            dna += a.name + '.'
        dna += self.name
        self.dna = DNA(dna)


# ********   *********   *********   *********   *********   *********   *********   *********
# ********   *********   *********   *********   *********   *********   *********   *********
# ********   *********   *********   *********   *********   *********   *********   *********
class Node(NodeAccessors, NodeConstructors):
    ''' A node is basically, any directory, file, or even column of file in the structure.
        Nodes are constructed with the following arguments:
            "name": [str] any string
            "depth" [int] the depth of the node, counting from the root (e.g. if root = C:/Users/alpha, and a node is C:/Users/alpha/beta/gamma, its depth = 2)
            "path": [str|pathlib.Path]
            "kind": any of  ['root', 'publisher', 'report', 'field', 'file', 'property']
            "parent": None|Node
            "fruit": None|pd.DataFrame|dict (of dict(s)) of pd.DataFrame(s)

        Each node is on its own, if not constructed through a Tree.
        Nodes constructed through the tree, have relations;

            parent
            children : Group (of Nodes)
            siblings: Group
            descendants: Group
            ascendants: Group
            dna: str
            fruit: a dataframe, or a dict of dataframes, or a dict of dicts of dataframes


        __call__ method:
            The call method on a node, returns the data from the database:
            The file(s) is read only once, and kept intact in memory, no matter how and which queries have been made to it afterwards.
                The database data is in tz-unaware UTC.

            Arguments: tz_pipe, start_date, end_date

            tz_pipe [str|list|None]:
                - None --> returns UTC, tz-unaware
                - str or [str]: returns data to this timezone (with the process: to_utc --> to_timezone
                - list: if list, the first argument MUST be UTC. Then, will apply tz conversions: to_utc --> to_timezone1 --> to_timezone2
                - ['UTC', 'some timezone'] --> returns data converted in "some timezone", tz-aware
                - ['UTC', 'some timezone', None] --> returns data converted in 'some timezone', tz-unaware

            start_date: None (start from the first available) or Datetime-like ('YYYY-MM-DD HH:MM')
            end_date: None (go till the last available) or Datetime-like ('YYYY-MM-DD HH:MM')
            *** start_date and end_date are applied AFTER the tz_pipe conversion (if any)

            If, you re-call a node, with no arguments (node()), it will return the tz_unaware UTC full data, regardless of whether or not
            you have once called with some combination of not-None tz_pipe, start-date, end_date.

            Return:

            If kind = report:
                {field1:{subfield1:df1, subfield2:df2}, field2:{subfield1:df1}} .. etc
            if kind = field:
                {subfield1:df1, subfield2:df2}
            if kind = file:
                df1
            if kind = property:
                df1[[self.name]]

    '''
    instances = []
    def __init__(self, name:str, depth:int, path:str|Path, kind:str, parent, fruit:dict|pd.DataFrame|None = None):
        self.instances.append(self)
        self.depth = depth
        self.name = name
        self.path = path
        self.kind = kind
        self.parent = parent
        self.fruit = fruit
        self.is_multiindex = False # added. Careful to establish it correctly in the AggDemandCurves

        # a bit messy, but solved the problem
        try:
            self.tree = inspect.currentframe().f_back.f_locals["self"]
        except:
            pass
        self.tz_history = []

    # ********   *********   *********   *********   *********   *********   *********   *********
    def depth2kind(self,depth):
        return self.tree.kinds[depth]
    # ********   *********   *********   *********   *********   *********   *********   *********
    def kind2depth(self, kind):
        reverse = {v:k for k,v in self.tree.kinds.items()}
        return reverse[kind]

    # ********   *********   *********   *********   *********   *********   *********   *********
    def first_call(self):

        if not isinstance(self.fruit, type(None)): # clause to prevent recursive read of parent/children if already read
            return

        if self.kind not in ['file', 'property']:

            df = {}
            for c in self.children:
                # print('From node: {} ({}), Calling child: {} ({})'.format(self, self.kind, c, c.kind))
                df[c.name] = c.first_call() # important: this should be c.first_call()

            self.fruit = df

        elif self.kind == 'file':
            df = self.reader(is_multiindex=self.is_multiindex)
            for child in self.children:
                if child.name not in df.columns:
                    # in partial-testing cases (e.g. database has 01&02 of Jan 2022, and new update is 03 of Jan)
                    # maybe (in henex usually), a subfield does not exist in the cached base, but exists in the new query
                    # e.g. Lignite volumes in crida markets.
                    df[child.name] = np.nan

                child.fruit = df.loc[:, child.name]
                if self.is_multiindex == False:
                    child.fruit = child.fruit.to_frame()

            self.fruit = df

        elif self.kind == 'property':
            self.parent.first_call()
            parent_df = self.parent()
            df = parent_df.loc[:, self.name].to_frame()# this is a view
            self.fruit = df

        else:
            raise Warning("Cannot call a node of kind: '{}'.".format(self.kind))

    # ********   *********   *********   *********   *********   *********   *********   *********
    def __call__(self, tz:str|None = None, start_date = None, end_date = None, truncate_tz = False, drop_trivial = True) -> pd.DataFrame | dict:

        if isinstance(self.fruit, type(None)):
            self.first_call()

        self.dispatch = self.fruit.copy()

        if self.kind not in ['file', 'property']:

            df = {}
            for c in self.children:
                # print('\t\tCalling child ', c)
                df[c.name] = c(tz, start_date, end_date, truncate_tz, drop_trivial)

        elif self.kind == 'file':
            df = self.fruit.copy()
            df = self.apply_tz_pipe(df, tz, truncate_tz)
            df = self.apply_date_filter(df, start_date, end_date, self.is_multiindex)
            # if drop_trivial:
            #     df = df.dropna(axis = 'columns', how='all')
            #     try:
            #         df = df.drop(columns = df.columns[df.sum()==0])
            #     except:
            #         pass
                # df = df.replace(0, np.nan)


        elif self.kind == 'property':
            if not isinstance(self.parent.fruit, type(None)):
                df = self.parent.fruit.loc[:, self.name].to_frame()
            else:
                parent_df = self.parent(tz, start_date, end_date, truncate_tz, drop_trivial)
                df = parent_df.loc[:, self.name].to_frame()# this is a view

        else:
            raise Warning("Cannot call a node of kind: '{}'.".format(self.kind))

        return df

    # ********   *********   *********   *********   *********   *********   *********   *********
    def apply_date_filter(self, df, start_date, end_date, is_multiindex = False, return_copy = False):
        ''' if return_copy = False, it will return a view of the original dataframe, within the specified date limits.
            This view can be plotted, exported to csv etc, but not modified.
        '''

        if return_copy:
            dff = df.copy()
        else:
            dff = df

        if start_date:
            if not is_multiindex:
                dff = df.loc[start_date:]
            else:
                dff = dff[dff.index.get_level_values(0) >= start_date]

        if end_date:
            if not is_multiindex:
                dff = dff.loc[:end_date]
            else:
                dff = dff[dff.index.get_level_values(0) <= end_date]

        return dff

    # ********   *********   *********   *********   *********   *********   *********   *********
    def plot(self, tz:str|None=None, start_date=None, end_date=None, kind='area', show = True, save_path = None, title = None, ylabel = None, xlabel = None, df = None, line_cols = None, area_cols = None, transformation = None):
        '''
        save_path : str|Path (directory or path/that/ends/with.html)
        '''


        if self.kind not in ['file', 'property']:
            raise AssertionError('\n--> You can only plot a node whose "kind" is "file" or "property". "{}" is of kind: {}'.format(self.dna, self.kind))

        if hasattr(self, 'dna'): # check, because synthetic nodes (from .combine()) don't have dnas
            print('\n\tPlotting node "{}"...'.format(self.dna))
        else:
            print('\n\tPlotting synthetic node "{}"...'.format(self.name))
        if save_path:
            save_path = Path(save_path)
            if save_path.suffix == ".html":
                pass
            else:
                save_path = (save_path / self.name).with_suffix('.html')

        if not isinstance(title, type(None)):
            title = self.parent.name + "." + self.name

        if isinstance(df, type(None)):
            df = self(tz, start_date, end_date)
        if transformation:
            df = transformation(df)

        if self.is_multiindex:
            print('\n\tCaution: If printing more than 2 or 3 days, the plot may become too slow.')
            fig = Plot.plot_agg_curves(df, xlabel=xlabel, ylabel=ylabel)
        else:
            if kind == 'area':
                fig = Plot.area_plot(df=df, show=show, save_path=save_path, title=title, ylabel = ylabel, xlabel=xlabel)
            elif kind == 'line':
                fig = Plot.line_plot(df=df, show=show, save_path=save_path, title=title, ylabel = ylabel, xlabel=xlabel)
            elif kind == 'multi-type':
                fig = Plot.multi_chart_type(df, show=show, save_path=save_path, title=title, ylabel=ylabel, line_cols=line_cols, area_cols=area_cols, xlabel=xlabel)

        print('\t\tPlotting Completed.')
        return fig

    # ********   *********   *********   *********   *********   *********   *********   *********
    def derive_if_file_is_multiindex(self):
        try:
            head = pd.read_csv(self.path, nrows=1, index_col=0, encoding='utf-8', sep=exso._list_sep, decimal=exso._decimal_sep)
            self.encoding = 'utf-8'
        except:
            head = pd.read_csv(self.path, nrows=1, index_col=0, encoding='utf-16', sep=exso._list_sep, decimal=exso._decimal_sep)
            self.encoding = 'utf-16'

        cols = head.columns.to_list()
        is_multiindex = any([c.startswith('Unnamed') for c in cols])
        self.is_multiindex = is_multiindex

    # ********   *********   *********   *********   *********   *********   *********   *********
    def reader(self, is_multiindex, tz: str | None=None):
        ''' Caution: If I read something once, with a specific tz_pipe,
            then, I fI re-call this, it won't be read again, because it stays as a fruit.
        '''
        
        if is_multiindex:
            df = self.read_multiindex(self.path, header=[0, 1], index_col=[0, 1], tz=tz)
        else:
            try:
                df = pd.read_csv(self.path, index_col=0, header=0, encoding='utf-8', sep=exso._list_sep, decimal=exso._decimal_sep)
            except:
                df = pd.read_csv(self.path, index_col=0, header=0, encoding='utf-16', sep=exso._list_sep, decimal=exso._decimal_sep)


            try:
                df.index = pd.to_datetime(df.index, format = exso._dt_format)
            except:
                try:
                    df.index = pd.to_datetime(df.index, format='%Y-%m-%d')  # e.g. ReservoirFilling rate (daily resolution YYYY-MM-DD)

                except:
                    df.index = pd.to_datetime(df.index, format='mixed')

        return df

    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def apply_tz_pipe(df, tz: str | None, truncate_tz = False):
        ''' if tz is None, dont apply any transformation.
            If tz = 'utc', then make it tz-naive (so, localize to utc)
            if tz = other, then, localize to UTC, and then convert to other.
            If truncate_tz, then after any conversions applied, localize to None (NOT convert to None)
        '''
        if not tz:
            return df

        if isinstance(tz, list):
            raise ValueError("tz (former tz_pipe) is a string or None argument. The tz_pipe list-based entry is no longer provided. Just enter the timezone you want to convert to, and optionally use the 'truncate_tz' argument.")



        localize_to = 'utc'
        if tz.lower() == 'utc':
            convert_to = None
        else:
            convert_to = tz

        if isinstance(df.index, pd.MultiIndex):
            extra = {'level':0}
        else:
            extra = {}

        df = df.tz_localize(localize_to, **extra)
        if convert_to:
            df = df.tz_convert(convert_to, **extra)

        if truncate_tz:
            df = df.tz_localize(None)

        return df

    # ********   *********   *********   *********   *********   *********   *********   *********
    def read_multiindex(self, filepath, header=[0,1], index_col = [0,1], nrows = None, tz:str|None=None):

        df = pd.read_csv(filepath, header=header, index_col=index_col, nrows=nrows, encoding='utf-8', sep=exso._list_sep, decimal=exso._decimal_sep)

        for i, cols_as_read in enumerate(df.columns.levels):
            columns_new = np.where(cols_as_read.str.contains('Unnamed'), '', cols_as_read)
            df.rename(columns=dict(zip(cols_as_read, columns_new)), level=i, inplace=True)

        _raw = df.copy()

        def to_datetime(x):
            return pd.Timestamp(x)

        df.index = df.index.set_levels([pd.to_datetime(df.index.levels[0]), df.index.levels[1]])
        df = Node.apply_tz_pipe(df, tz)

        return df

    # ********   *********   *********   *********   *********   *********   *********   *********
    def export(self, to_path, tz:str|None = None, start_date = None, end_date = None, truncate_tz=False):
        '''
        to_path: str or Path
            if the node is a file, the "to_path" can be either a filepath, or a directory (in which, a fille called <self.name>.csv will be created)
            If the node is a directory, the "to_path" must be a directory
        '''

        if Path(to_path) in self.ascendants.path:
            raise IOError("The target path cannot be within the exso-database. (Given path was: {})".format(to_path))

        if isinstance(to_path, str):
            to_path = Path(to_path)

        if self.kind == 'file':
            df = self(tz= tz, start_date = start_date, end_date=end_date, truncate_tz=truncate_tz)


            # user entered a custom path, and directly called for a file export. so, respect that
            if to_path.suffix == '.csv':
                target = to_path

            # if the user entered a directory as a path, and directly called a file-export, the name of the file will probably not be in the name.
            # e.g. scada.scada.load.export(to_path = 'path/to/desired/dir')
            # will be made to: 'path/to/desired/dir/load.csv'
            elif self.name not in to_path.name:
                target = (to_path / self.name).with_suffix('.csv')
            else:
                target = to_path.with_suffix('.csv')

            target.parent.mkdir(exist_ok=True, parents=True)

            df.to_csv(target, sep=exso._list_sep, decimal=exso._decimal_sep, date_format=exso._dt_format)

        else:
            to_path.mkdir(exist_ok=True, parents = True)
            for child in self.children:
                child_dir = to_path / child.name
                child.export(child_dir, tz, start_date, end_date, truncate_tz)


# ********   *********   *********   *********   *********   *********   *********   *********
# ********   *********   *********   *********   *********   *********   *********   *********
# ********   *********   *********   *********   *********   *********   *********   *********
class Group:
    ''' A wrapper for nodes
        Allows to access the attributes of the Node class,
        for all members of the Group
    '''
    def __init__(self, node_objects):
        self._members = node_objects
    # ********   *********   *********   *********   *********   *********   *********   *********
    def truncate(self, to, unique_only=True):

        truncated = [m.truncate(to) for m in self]
        truncated = [t for t in truncated if t]

        if unique_only:
            unique = []
            [unique.append(m) for m in truncated if m not in unique]
            return Group(unique)
        else:
            return Group(truncated)

    # ********   *********   *********   *********   *********   *********   *********   *********
    def get_relevants(self, kind, reduce = True):
        one_to_one = [m.get_relevants(kind)[0] for m in self]
        if not reduce:
            return Group(one_to_one)
        else:
            results = []
            for r in one_to_one:
                if r not in results:
                    results.append(r)
            return Group(results)

    # ********   *********   *********   *********   *********   *********   *********   *********
    def __iter__(self):
        return iter(self._members)
    # ********   *********   *********   *********   *********   *********   *********   *********
    def __repr__(self):
        return [s.name for s in self]
    # ********   *********   *********   *********   *********   *********   *********   *********
    def __str__(self):
        return str([s.name for s in self])
    # ********   *********   *********   *********   *********   *********   *********   *********
    def __getitem__(self, item):
        ret = self._members[item]
        return ret
    # ********   *********   *********   *********   *********   *********   *********   *********
    def __getattr__(self, item):
        return [m.__getattribute__(item) for m in self]
    # ********   *********   *********   *********   *********   *********   *********   *********
    def __len__(self):
        return len(self._members)
    # ********   *********   *********   *********   *********   *********   *********   *********
    def __add__(self, other):
        if isinstance(other, Group):
            self._members = self._members + other._members
        elif isinstance(other, list):
            self._members = self._members + other
    # ********   *********   *********   *********   *********   *********   *********   *********
