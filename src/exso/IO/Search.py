import numpy as np
from exso.IO.Nodes import Group
from fuzzywuzzy import process


# ********   *********   *********   *********   *********   *********   *********   *********
# ********   *********   *********   *********   *********   *********   *********   *********
# ********   *********   *********   *********   *********   *********   *********   *********
class Search:

    # ********   *********   *********   *********   *********   *********   *********   *********
    def search(self, *strings, n_best = 10, kind = None):

        self.i = 0

        ordered_strings = self.optimize_order(*strings)
        use_nodes = None
        for s in ordered_strings:
            s_candidates = self._search(s, use_nodes, n_best=500)
            s_dnas = [tup[0] for tup in s_candidates] #ordered by relevance
            use_nodes = Group(self.get_nodes_whose('dna', equals=s_dnas)) # not ordered

        if kind:
            nodes = self.get_nodes_whose('dna', equals=s_dnas)
            nodes = nodes.truncate(to=kind)
            nodes = Group(nodes[:n_best])

        else:
            s_dnas = s_dnas[:n_best]
            nodes = Group(self.get_nodes_whose('dna', s_dnas))

        return nodes

    # ********   *********   *********   *********   *********   *********   *********   *********
    def _search(self, string, use_nodes = None, n_best = 5):

        if isinstance(use_nodes, type(None)):
            dna_list = self.nodes.dna
        else:
            dna_list = use_nodes.dna
        choices = np.unique(dna_list)

        candidates = process.extract(string, choices, limit = n_best,)
        self.i += 1
        return candidates


    # ********   *********   *********   *********   *********   *********   *********   *********
    def optimize_order(self, *strings):
        depths = []
        for s in strings:
            s_candidates = self._search(s, use_nodes=None, n_best=1)
            best_candidate_dna = s_candidates[0][0]
            best_candidate = self.get_nodes_whose('dna',equals =[best_candidate_dna], collapse_if_single = True)
            depths.append(best_candidate.depth)

        ''' correct order is first search for the deepest, then the second deepest, etc'''
        ordered_strings = [s for s,d in sorted(zip(strings, depths),key=lambda x: -x[1])]
        return ordered_strings

    # ********   *********   *********   *********   *********   *********   *********   *********
    def is_found_in_reports(self, nodes):
        found_in_reports = Group([s.get_ascendants_of_depth(depth=2) for n in nodes])
        return found_in_reports

    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
