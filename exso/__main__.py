import sys, os
import argparse
import traceback

import haggis.string_util

import exso
from pathlib import Path


def main():
    print('\n(Resizing terminal)')
    os.system('mode con: cols=200 lines=40')

    args = sys.argv[1:]
    p = argparse.ArgumentParser(prog="py -m exso")
    p.add_argument("mode", choices=["info", "update", "validate", "query", "set_system_formats"])
    p.add_argument("-rl", "--root_lake", default=None)
    p.add_argument("-rb", "--root_base", default=None)

    p.add_argument('--which', nargs='+', default="all",
                   help="--which argument can be either 'all' (default), or a list of valid report-names (space-separated)")
    p.add_argument('--exclude', nargs='+', default = None,  help= 'specify report name(s) to exclude from the update process')
    p.add_argument('--publishers', nargs='+', default = None, choices=['admie', 'henex', 'entsoe'])
    p.add_argument('--groups', nargs='+', default=None, choices=['ISPResults',
                                                                 'ISPForecasts',
                                                                 'ISPRequirements',
                                                                 'Forecasts',
                                                                 'UnitAvailabilities',
                                                                 'Transmission',
                                                                 'Balancing',
                                                                 'DAS',
                                                                 'Hydro',
                                                                 'SCADA',
                                                                 'DAM',
                                                                 'IntraDayMarket',
                                                                 'DemandSupplyBids',
                                                                 'Gas'
                                                                 ])

    p.add_argument('--val_report', help='report name you wish to validate.')
    p.add_argument('--val_dates', nargs='+', help="space separated date(s) to validate. format: YYYY-M-D")
    p.add_argument('--val_fields', nargs='+', default=None,
                   help='"Field(s)" are the filenames, as to be found in the database folder of a specific report (space separated).')

    p.add_argument('-loc', '--query_locator',
                   help="'locator' means a unique identifier of database objects. \nexample: root.admie.isp1ispresults, will extract the whole database of this report and transform it / slice it depending on the rest of the options you set.")
    p.add_argument('-output_dir', '--query_output_dir', default=None,
                   help='If specified, it will be used to save the generated plot (if -plot), and/or the extracted timeslice (if -extract).')
    p.add_argument('-tz', '--query_tz', default='EET')
    p.add_argument('-from', '--query_from', help="Start date(time) of query (YYYY-M-D [H:M])")
    p.add_argument('-until', '--query_until', help="End date(time) of query (YYYY-M-D [H:M])")
    p.add_argument('-extract', '--query_extract', action='store_true',
                   help="If added, it means you wish to EXTRACT the specified query (among possible other actions)")
    p.add_argument('-plot', '--query_plot', action='store_true',
                   help="If added, it means you wish to PLOT the upstream query (among possible other actions)")
    p.add_argument('-stacked', '--plot_stacked', action='store_true', default=False,
                   help="If added, it means you wish the PLOT specified, to be a stacked-area plot")

    p.add_argument('--decimal_sep', default='.')
    p.add_argument('--list_sep', default=',')


    arguments = p.parse_args(args)

    if arguments.mode == "set_system_formats": # modify permanently
        exso._set_system_formats(decimal_sep = arguments.decimal_sep,
                                    list_sep = arguments.list_sep)
    else: # modify just once
        exso._decimal_sep = arguments.decimal_sep
        exso._list_sep = arguments.list_sep

    for attr in ['root_lake', 'root_base', 'query_output_dir']:
        if getattr(arguments, attr):
            setattr(arguments,attr, Path(getattr(arguments,attr)))


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
                           which = arguments.which,
                           exclude = arguments.exclude,
                           groups = arguments.groups,
                           publishers = arguments.publishers
                           )
        upd.run()

    elif arguments.mode == 'query':
        tree = exso.Tree(root_path = arguments.root_base)
        if not arguments.query_locator:
            print("The mode was 'query' but you didn't provide a query locator.")
            sys.exit()


        if arguments.query_tz:
            tz = arguments.query_tz
        else:
            tz = None

        save_dir = arguments.query_output_dir
        node = tree[arguments.query_locator]

        if arguments.query_extract:
            if not save_dir:
                print("\nYou must specify an output_dir when in -extract mode")
                input('Failed.')
                sys.exit()

            node.export(to_path = save_dir,
                        tz= tz,
                        start_date = arguments.query_from,
                        end_date = arguments.query_until)

        if arguments.query_plot:
            kind = 'line'
            if arguments.plot_stacked:
                kind = 'area'

            node.plot(tz=tz,
                      start_date=arguments.query_from,
                      end_date=arguments.query_until,
                      save_path=save_dir,
                      kind=kind)


    elif arguments.mode == 'validate':

        val = exso.Validation(report_name=arguments.val_report,
                              dates=arguments.val_dates,
                              root_lake=arguments.root_lake,
                              fields=arguments.val_fields)
        val.run()



if __name__ == '__main__':
    try:
        main()
        print()
        print('\n\nSuccessful.')
        print(haggis.string_util.make_box('Thanks for using exso!', vertical_padding=2, horizontal_padding=3))
        print('\nYou can support the project through a number of ways:'
              '\n\t1. Visit the github page (https://github.com/ThanosGkou/exso) and put a star to the project (on the top right corner). You can sign-in even with a google account.'
              '\n\t2. Share the project with your colleagues'
              '\n\t3. Cite the project when it contributes to your work'
              '\n\t4. Become a sponsor: https://github.com/sponsors/ThanosGkou')
        input('\nHit any key to continue...')
    except SystemExit as ex: # --help mode
        pass
    except:
        print()
        print('An error occured')
        print(traceback.format_exc())
        input('\n\nFailed...')


