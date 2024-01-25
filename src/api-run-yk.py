import exso

root_lake='datalake'
root_base ='database'
reports=['DAM_Results']

upd = exso.Updater(root_lake, root_base, some = reports)

upd.run(use_lake_version = 'latest')
