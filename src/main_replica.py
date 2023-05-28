import sys
import argparse

import exso
from pathlib import Path


def main():
    args = sys.argv[1:]
    p = argparse.ArgumentParser(prog="ExSO CLI Update")
    p.add_argument("mode", choices=["info", "update", "validate", "query"], default="update")
    p.add_argument("-rl", "--root_lake")
    p.add_argument("-rb", "--root_base")

    p.add_argument('--which',nargs = '+', default="all", help="--which argument can be either 'all' (default), or a list of valid report-names (space-separated)")

    p.add_argument('--val_report', help = 'report name you wish to validate.')
    p.add_argument('--val_dates', nargs='+', help="space separated date(s) to validate. format: YYYY-M-D")
    p.add_argument('--val_fields', nargs = '+', default=None, help='"Field(s)" are the filenames, as to be found in the database folder of a specific report (space separated).')

    p.add_argument('-loc', '--query_locator', help="'locator' means a unique identifier of database objects. \nexample: root.admie.isp1ispresults, will extract the whole database of this report and transform it / slice it depending on the rest of the options you set.")
    p.add_argument('-output_dir', '--query_output_dir')
    p.add_argument('-tz', '--query_tz', choices=['EET', 'CET', None])
    p.add_argument('-from', '--query_from', help="Start date(time) of query (YYYY-M-D [H:M])")
    p.add_argument('-until', '--query_until', help = "End date(time) of query (YYYY-M-D [H:M])")
    p.add_argument('-extract', '--query_extract', action='store_true')
    p.add_argument('-plot', '--query_plot', action='store_true')
    p.add_argument('-stacked','--plot_stacked', action='store_true', default=False)

    arguments = p.parse_args(args)
    for attr in ['root_lake', 'root_base', 'query_output_dir']:
        if getattr(arguments, attr):
            setattr(arguments,attr, Path(getattr(arguments,attr)))

    if arguments.which == "all":
        all = True
        some = None
    else:
        all = False
        some = arguments.which




    if arguments.mode == 'info':
        rp = exso.Report.Pool()
        print()
        print('Available reports are:')
        avail = rp.get_available()
        avail = avail.drop(columns=['is_implemented', 'official_comment'])
        print(avail)
        print()
        print('-' * 50)
        for k,v in rp.get_text_description().items():
            print(k)
            print('\tDescr:', v)
            print('-'*50)
        print('-'*50)

    if arguments.mode == 'update':
        upd = exso.Updater(root_lake=arguments.root_lake,
                           root_base=arguments.root_base,
                           all=all,
                           some = some
                           )
        upd.run()

    elif arguments.mode == 'query':
        tree = exso.Tree(root_path = arguments.root_base)
        tree.make_tree()
        if not arguments.query_locator:
            print("The mode was 'query' but you didn't provide a query locator.")
            sys.exit()

        save_dir = arguments.query_dir

        if arguments.query_tz:
            tz_pipe = ['UTC', arguments.query_tz, None]
        else:
            tz_pipe = None

        node = tree[arguments.query_locator]
        if arguments.query_extract:
            if arguments.root_base in arguments.query_output_dir.parents:
                print()
                print('You mustnt distract the database (or the datalake) directories. Store somewhere else.')
                sys.exit()

            node.export(to_path = arguments.query_output_dir,
                        tz_pipe = tz_pipe,
                        start_date = arguments.query_from,
                        end_date = arguments.query_until)

        if arguments.query_plot:
            plot_savepath = None
            if arguments.query_plot:
                if save_dir:
                    plot_savepath = (save_dir/ arguments.query_locator).with_suffix('.html')

            node.plot(tz_pipe=tz_pipe,
                      start_date = arguments.query_from,
                      end_date = arguments.query_until,
                      save_path = plot_savepath,
                      area = arguments.plot_stacked)


    elif arguments.mode == 'validate':

        val = exso.Validation(report_name=arguments.val_report,
                              dates=arguments.val_dates,
                              root_lake=arguments.root_lake,
                              fields=arguments.val_fields)
        val.run()



if __name__ == '__main__':
    main()
