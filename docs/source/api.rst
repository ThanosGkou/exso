
.. _api:

API
===


.. include:: substitutions.rst

.. _python_api:
Python API
"""""""""""

exso.Updater class
------------------
The class for performing datalake/database updates.

::

    exso.Updater(root_lake:str|Path|None=None,
                 root_base:str|Path|None=None,
                 which:str|list|None = None,
                 exclude:str|list|None=None,
                 groups:str|list|None = None,
                 publishers:str|list|None = None)

    # param root_lake: [str|Path|None] The desired path to use as the datalake directory
    #   If None, default path for windows is: C:\Users\<username>\Desktop\exso_data\datalake
    # param root_base: [str|Path|None] the desired path to use as the database directory
    #   If None, default path for windows is: C:\Users\<username>\Desktop\exso_data\database


    # param which (optional): If given, will update only these report name(s) *
    # param exclude (optional): If given, will NOT update these report groups *
    # param publishers (optional): If given, will update only reports of this/these publishers *
    # param groups (optional): If given, will update only these report group(s) *

* **The Parameters: which, exclude, groups, publishers are intersectional filters.**


::

    # Example
    import exso
    root_lake = r"path\to\desired\datalake\directory" # e.g. r"C:\Users\your_username\exsodata\datalake"
    root_base = r"path\to\desired\database\directory" # e.g. r"C:\Users\your_username\exsodata\database"

    upd = exso.Updater(root_lake, root_base)
    upd.run()


exso.Tree class
------------------
The class for locating specific nodes from the database (and more).
* Docs on what are :ref:`Locators and how to use them <node_locators>`

::

    tree = exso.Tree(root_path:Path|str|None)

Tree.combine
------------
Allows for combining multiple nodes to a single node. Applies only for 'file'-kind Nodes


::

    synthetic_node = tree.combine(*locators,
                                   with_name:str|None = None,
                                   handle_synonymity:str|list = 'auto',
                                   resolution = 'auto'))

    # param *locators: a list|tuple of the node locators you want to combine (locator1, locator2, ..., locatorN). Locators must be of the kind 'file'
    # param with_name: the name of the new synthetic node.
    # param handle_synonymity: when combining nodes, it's possible to end up with naming conflicts (property1 of locator1 having the same name as propertyN of locatorM).
        if 'auto', and if such conflict emerges, the returned node will have properties with suffixes (prop_locator1, prop_locatorN)
        Else, you can provide a list of suffixes to be applied for each locator, which will only be applied if such conflict arises
    # param resolution: How to handle nodes with different datetime resolutions. Not really suggested to put anything other than 'auto'


Tree.visualize
---------------
Displays a visual representation of the database structure

::

    tree.visualize()


Tree.__getitem__
-----------------
The main method to access nodes of the database through node-locators. Checkout Locators and how to use them :ref:`here <node_locators>`


::

    node = tree[locator]

    # e.g.
    node = tree['root.henex.dam_results.dam_results.results']

    # or:
    node = tree['dam_results.>>']


exso.Node class
-----------------
::
    .__call__()

    .plot()

    .export()




.. _cli:
Command Line API
""""""""""""""""""
In order to use exso through the command line:
* Launch a terminal and activate the virtual environment where exso is installed
* Use the Command-Line API options:

>>> (venv) python -m exso [--args]

- Example for update mode:
>>> (venv) python -m exso update -rl "path/to/datalake" -rb "path/to/database" --which ISP1ISPResults

- Default database and datalake locations are:
    - datalake: Desktop\exso_data\datalake
    - database: Desktop\exso_data\database

Below lies the list with all options for the command-line api of exso, which is accessible through:

>>> (venv) python -m exso --help

::


    positional arguments:
      {info,update,validate,query,set_system_formats}

    options:
      -h, --help            show this help message and exit
      -rl, --root_lake ROOT_LAKE
      -rb, --root_base ROOT_BASE
      --which WHICH [WHICH ...]
                            --which argument can be either 'all' (default), or a list of valid report-names (space-separated)
      --exclude EXCLUDE [EXCLUDE ...]
                            specify report name(s) to exclude from the update process
      --publishers {admie,henex,entsoe} [{admie,henex,entsoe}]

      --groups
      -loc, --query_locator QUERY_LOCATOR
                            'locator' means a unique identifier of database objects. example: root.admie.isp1ispresults, will extract the whole database of this report and transform it / slice it
                            depending on the rest of the options you set.
      -output_dir, --query_output_dir QUERY_OUTPUT_DIR
                            If specified, it will be used to save the generated plot (if -plot), and/or the extracted timeslice (if -extract).

      -tz, --query_tz QUERY_TZ

      -from, --query_from QUERY_FROM
                            Start date(time) of query (YYYY-M-D [H:M])
      -until, --query_until QUERY_UNTIL
                            End date(time) of query (YYYY-M-D [H:M])
      -extract, --query_extract
                            If added, it means you wish to EXTRACT the specified query (among possible other actions)
      -plot, --query_plot   If added, it means you wish to PLOT the upstream query (among possible other actions)
      -stacked, --plot_stacked
                            If added, it means you wish the PLOT specified, to be a stacked-area plot
      --decimal_sep DECIMAL_SEP
      --list_sep LIST_SEP


|xlsm| API
-----------

As of version 1.0.0, you can use exso through an excel-based gui, for basic exso-functionality:

* You can :ref:`Download and Install exso directly from ExSO.xlsm <install_with_xlsm>`
* You can specify the reports to update in the "Reports Selection" Sheet.
* Launch an update mode or query through buttons and option boxes

:fig:`figs\exso_xlsm_2.png`
