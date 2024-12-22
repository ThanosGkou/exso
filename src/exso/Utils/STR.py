import re
from itertools import islice
from pathlib import Path

import pandas as pd


###############################################################################################
###############################################################################################
###############################################################################################
class STR:
    # ********   *    ********   *    ********   *    ********   *   ********
    # ********   *    ********   *    ********   *    ********   *   ********
    @staticmethod
    def list2string(input_list, sep='\n', indent='\t', add_sep_in_first=True, ):
        text = ""
        for i, val in enumerate(input_list):
            if i == 0:
                if add_sep_in_first:
                    text += sep + indent + str(val)
                else:
                    text += indent + str(val)
            else:

                text += sep + indent + str(val)

        return text
    # ********   *    ********   *    ********   *    ********   *   ********
    # ********   *    ********   *    ********   *    ********   *   ********

    @staticmethod
    def iterprint(dict_to_print, indent:int|str=2, return_text=False, prev_text=""):
        ws = ' '
        if isinstance(indent, int):
            pass
        else:
            indent = len(ws)

        for k, v in dict_to_print.items():

            if isinstance(v, dict):
                row = indent * ws + f'{str(k):<25}' + f'{"-->":<10}'  # + f'{str(v):<25}')
                if return_text:
                    prev_text += row + '\n'
                else:
                    print(row)

                prev_text = STR.iterprint(v, indent=indent + 2, return_text=return_text,
                                          prev_text=prev_text)

            elif isinstance(v, pd.DataFrame):
                if return_text:
                    row = indent * ws + f'{str(k):<25}' + f'{"-->":<10}'  # + f'{str(v):<25}')
                    prev_text += row + '\n'
                else:
                    print(row)
                prev_text = STR.df_to_string(v, indent=indent*ws, return_text=return_text)

            else:
                row = indent*ws + f'{str(k):<25}' + f'{"-->":<10}' + f'{str(v):<25}'
                if return_text:
                    prev_text += row + '\n'
                else:
                    print(row)


        if return_text:
            valid_text = re.sub(r'\n\+', '\n', prev_text)
            return '\n' + valid_text

    # ********   *    ********   *    ********   *    ********   *   ********
    @staticmethod
    def df_to_string(df, indent=2, return_text = True):
        if isinstance(indent, str):
            pass
        else:
            indent = " " * indent
        df_str = indent + df.to_string().replace("\n", "\n" + indent)
        if not return_text:
            print(df_str)
        return df_str

    # ********   *    ********   *    ********   *    ********   *   ********
    # ********   *    ********   *    ********   *    ********   *   ********
    @staticmethod
    def printProgressBar(iteration, total, prefix='', suffix='', decimals=1, length=50, fill='█', printEnd="\r"):
        """
        Call in a loop to create terminal progress bar
        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            length      - Optional  : character length of bar (Int)
            fill        - Optional  : bar fill character (Str)
            printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
        """
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        rprint(f'\r{prefix} |[green]{bar}| {percent}% [bold purple]{suffix}', end=printEnd)
        # Print New Line on Complete
        if iteration == total:
            print()

    # ********   *    ********   *    ********   *    ********   *   ********
    # ********   *    ********   *    ********   *    ********   *   ********

    @staticmethod
    def tree(dir_path: Path, level: int = -1, limit_to_directories: bool = False,
             length_limit: int = 1000):
        """Given a directory Path object print a visual tree structure"""
        space = '    '
        branch = '│   '
        tee = '├── '
        last = '└──'
        dir_path = Path(dir_path)  # accept string coerceable to Path
        files = 0
        directories = 0

        def inner(dir_path: Path, prefix: str = '', level=-1):
            nonlocal files, directories
            if not level:
                return  # 0, stop iterating
            if limit_to_directories:
                contents = [d for d in dir_path.iterdir() if d.is_dir()]
            else:
                contents = list(dir_path.iterdir())
            pointers = [tee] * (len(contents) - 1) + [last]
            for pointer, path in zip(pointers, contents):
                if path.is_dir():
                    yield prefix + pointer + path.name
                    directories += 1
                    extension = branch if pointer == tee else space
                    yield from inner(path, prefix=prefix + extension, level=level - 1)
                elif not limit_to_directories:
                    yield prefix + pointer + path.name
                    files += 1

        print(dir_path.name)
        iterator = inner(dir_path, level=level)
        for line in islice(iterator, length_limit):
            print(line)
        if next(iterator, None):
            print(f'... length_limit, {length_limit}, reached, counted:')
        print(f'\n{directories} directories' + (f', {files} files' if files else ''))



###############################################################################################
###############################################################################################
###############################################################################################
