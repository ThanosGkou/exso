import re

import numpy as np


###############################################################################################
###############################################################################################
###############################################################################################
class Similarity:
    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def jaccard(s1, s2):
        s1 = set(s1)
        s2 = set(s2)
        inter = s1.intersection(s2)
        jac = len(inter) / (len(s1) + len(s2) - len(inter))
        return jac

    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def map_names(key_list, value_list):
        mapper = {}
        for p in key_list:
            lookup_list = list(map(refine, value_list))
            p_orig = p
            p = p.lower()
            simils_jac = list(map(lambda x: jaccard(p, x), lookup_list))
            fit_jac = value_list[np.argmax(simils_jac)]
            simil_jac = np.max(simils_jac)
            mapper[p_orig] = fit_jac
        else:
            return mapper

    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def find_best_match2(lookup_list, string, n_best=None, return_indices=False, sort_by_simil=True):
        search_string = Similarity.refine(string)
        simils_jac = []
        for i in range(len(lookup_list)):
            lookup_list_ = Similarity.splitter(lookup_list[i])
            lookup_list_ = list(map(Similarity.refine, lookup_list_))
            sub_simils_jac = list(map(lambda x: Similarity.jaccard(search_string, x), lookup_list_))
            simils_jac.append(sum(sub_simils_jac))
        else:
            if return_indices:
                combo = [(
                 k, v) for k, v in enumerate(simils_jac)]
                if sort_by_simil:
                    combo.sort(key=(lambda x: x[1]), reverse=True)
            else:
                combo = [(
                 k, v) for k, v in zip(lookup_list, simils_jac)]
                if sort_by_simil:
                    combo.sort(key=(lambda x: x[1]), reverse=True)
            if n_best:
                return combo[:n_best]
            return combo

    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def splitter(string, return_list=True):
        splitters = re.findall('(?<=[a-z])[A-Z]|(?<!^)[A-Z](?=[a-z])', string)
        listed = re.split('(?<=[a-z])[A-Z]|(?<!^)[A-Z](?=[a-z])', string)

        for i in range(len(listed)):
            if i == 0 or len(splitters) == 0:
                pass
            else:
                listed[i] = splitters[i - 1] + listed[i]

        if return_list:
            return listed
        else:
            return " ".join(listed)
    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def refine(x):
        x = x.strip()
        x = re.sub(' +', ' ', x)
        x = re.sub('[()-]', '', x)
        x = x.lower()
        return x

    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def find_best_match(lookup_list, string, n_best=None, return_indices=False, sort_by_simil=True):
        lookup_list_ = list(map(Similarity.refine, lookup_list))
        search_string = Similarity.refine(string)
        simils_jac = list(map(lambda x: Similarity.jaccard(search_string, x), lookup_list_))
        if return_indices:
            combo = [(
             k, v) for k, v in enumerate(simils_jac)]
            if sort_by_simil:
                combo.sort(key=(lambda x: x[1]), reverse=True)
        else:
            combo = [(
             k, v) for k, v in zip(lookup_list, simils_jac)]
            if sort_by_simil:
                combo.sort(key=(lambda x: x[1]), reverse=True)
        if n_best:
            return combo[:n_best]
        return combo

###############################################################################################
###############################################################################################
###############################################################################################
