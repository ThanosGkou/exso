
.. _api:

API
===


.. _python_api:
Python API
"""""""""""

Update Mode
-----------
The below script will download and insert to the database all (63) currently supported reports. For more information continue reading.

::

    import exso
    # define desired datalake and database locationsto be stored in the disk
    # root_lake and root_base can also be pathlib.Path objects
    root_lake = r"path\to\desired\datalake\directory" # e.g. r"C:\Users\your_username\exsodata\datalake"
    root_base = r"path\to\desired\database\directory" # e.g. r"C:\Users\your_username\exsodata\database"

    upd = exso.Updater(root_lake, root_base)
    upd.run()

exso.Updater class
------------------
The class for performing datalake/database updates.

::

    exso.Updater(root_lake, root_base,
                 which:str|list|None = None,
                 exclude:str|list|None=None,
                 groups:str|list|None = None,
                 publishers:str|list|None = None)

    # param root_lake: [str|Path] The desired path to use as the datalake directory
    # param root_base: [str|Path] the desired path to use as the database directory

    # param which (optional): If given, will update only these report name(s) *
    # param exclude (optional): If given, will NOT update these report groups *
    # param publishers (optional): If given, will update only reports of this/these publishers *
    # param groups (optional): If given, will update only these report group(s) *

* **The Parameters: which, exclude, groups, publishers are intersectional filters.**


exso.Tree class
------------------
The class for locating specific nodes from the database (and more).
* Docs on what are :ref:`Locators and how to use them <node_locators>`

::

    tree = exso.Tree(root_path:Path|str|None)

Useful methods:
::

    # Tree.combine
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


::

    # Tree.visualize --> Displays a visual representation of the database structure
    tree.visualize()



::

    # Tree.__getitem__   Access a node from the database
    node = tree[locator]


exso.Node class
-----------------
::
    .__call__()

    .plot()

    .export()




.. _cli:
Command Line API
""""""""""""""""""


