![image alt ><](/docs/source/figs/logos/geodetic.png)


# ExSO

An analytical framework for the Greek and European Power&Gas, System Operation ("SO") & Market Exchange ("Ex").


-----
## Description

**ExSO** provides an integrated framework for retrieving, extracting, transforming, loading and analyzing time-aware data for the Greek and European Power&Gas sector.

It was developed as a private project, focusing on the Greek power & gas system. On the same architecture, support for Entso-e data was added, while more data-sources being planned for integration. 

([ENTSO-e's transparency platform](https://transparency.entsoe.eu/) was a game-changer on pan-european transparency. **ExSO**'s value to this end, consists of enabling automated updates, robust data storage, retrieval, transformations and visuals)

- The core of the project is the provision of an automated, versatile and robust framework for:
  - Downloading raw files ("the Datalake"), as reported by Power&Gas Publishing Entities (ENTSO-e, ADMIE/IPTO, HEnEX, Desfa, ...)
  - Compiling raw, sparse files into flat, clean, high-quality timeseries
  - Inserting/updating the parsed data to a local, self-maintained database ("the Database")
  - Providing an API for accessing, slicing, transforming, analyzing, and visualizing the local Database.


- The local database consists of a tree structure of local directories and .csv files. The reasons why we opted for csv-based format are aligned with the [Rationale](#rationale) of the project:
  - Anyone can access a csv file without needing programming or SQL skills (wider access)
  - No local/remote database server required (portability)
  - No significant loss of speed (good enough efficiency)


-----
## What's New

Checkout [new features](https://exso.readthedocs.io/en/latest/whats_new.html) and  [ExSO Official Documentation](https://exso.readthedocs.io/en/latest/)

-----
## Rationale
**Publicly-available does not always mean publicly-accessible**
- Market players, TSOs, and professionals in the energy sector may or may not already have access to some of the data made accessible by **ExSO**, through paid or "members only" subscriptions (e.g. market participants).
- Individuals, researchers, and in (surprisingly) many cases professionals are either not entitled, or not willing to pay for high-quality data access.
- Even when an interested party is willing to pay for high-quality, long-term timeseries data, it's not clear where would he/she attend to.
- To our knowledge, no commercial or "members-only" database provides any of the variety, the duration, the reliability and the transparency that **exso** provides.
- We strongly believe in open access and transparency. **ExSO** is a project aiming to render publicly-available data in the scope of the Power&Gas sector, utilizable and accessible by anyone, expert or not.

-----
## Main Features

- Get **information** on implemented *Reports*, their content, their availability periods, metadatata, etc.
  - A *Report* is any set of properties, according to the way it's being published (e.g. Bulgarian Load @entsoe, ISP1ISPResults - Integrated Scheduling Process @admie, etc.)
- **Create** a local database of Market and System data (flat, seamless timeseries over the whole availability interval of each report)
- **Update** (hot/cold-start) the datalake and database for all or some of the implemented reports
- Interactive **Visualization**
- **Time-slicing** operations (timezone change, from/to time slicing)
- **Exporting/Extracting** visualizations and/or time-sliced data to a "sandbox" location (data in the database should not be modified in any way)

-----
## Basic Usage & Installation

For more info on installation and usage, please review the [ExSO Official Documentation](https://exso.readthedocs.io/en/latest/)

Installation can be now done semi-automatically through the [Excel-based GUI](https://github.com/ThanosGkou/exso/tree/main/ExSO.xlsm)

```sh
# install exso
(venv) pip install exso
```

```sh
import exso

root_datalake = "path/to/datalake"
root_database = "path/to/database"

# Run the update functionality

reports_to_update = ['ISP1ISPResults', 'DAM_ResultsSummary']
updater = exso.Updater(root_datalake, root_database, which = reports_to_update)
updater.run()

# Access & Combine nodes
isp1_thermal_schedule = t['isp1ispresults.isp_schedule.thermal']

combo_node = t.combine(('isp1ispresults.isp_schedule.thermal',
                        'dam_resultssummary.buy.mcp'))
                        
# Plot, Retrieve, Export
start_date = '2022-6-1'
end_date = '2022-6-10'
tz = 'EET'

fig = isp1_thermal_schedule.plot(start_date=start_date, end_date=end_date, tz = tz,
                                 kind = 'area')


combo_node.export(to_path = "some/directory/or/filepath",
                  tz = tz,
                  truncate_tz = True)
                  
```

![plotly_viz.png](resources/plotly_viz.png)

----
## Issues
- Feel free to submit any issues [here](https://github.com/ThanosGkou/exso/issues) or via e-mail
- Use of ***ExSO*** (update mode) with conda environments, was reported to present encoding issues during printing coloured text in the console, and despite otherwise being functional, the print statement after each report's success causes an encoding error. Try to use pip environments instead.


----
## Roadmap

**ExSO remains the only available high-quality, free and open-source data framework for the greek power market and system operation.**


The first milestone, which will require a fair amount of refactoring is the June of 2025, when, the Market Time Unit for the SDAC countries will beecome 15-minute (from 1 hour currently)

But, with relatively small effort - small relative to its impact -, exso can become much larger:
- Incorporate all entsoe countries
- Incorporate data for the greek natural gas system
- Introduce a free /freemium online platform to serve the data, while still remaining open source


----
## Support ExSO
You can support the project through a number of ways:
1. Put a star on the [project's page](https://github.com/ThanosGkou/exso) on the top right corner! (You can sign-in even with a google account)
2. Share and [cite](#citation) the project where and when applicable
4. Become a [sponsor](https://github.com/sponsors/ThanosGkou) to acccelerate the [Roadmap](#roadmap), or to request custom features
5. Issue reporting: It's always helpful to report any bugs and issues.
6. Collaboration: ExSO could benefit from various collaborations, less or more technical:
   - Documentation and project appearance
   - Data Documentation
   - Co-development of a publicly available data server
   - Expand the Reporting/Analytic capabilities of ExSO
   - Make the exso.DataBase a unique module (exsoDB)

    Feel free to reach out for questions and requests!


----
## License

<a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License (CC BY-NC-ND 4.0)</a>

Briefly (without this description being a substitute for the full license or any of its clauses):

**You are free to**:

- Use — Download/Install/Deploy ***exso***
- Share — copy and redistribute the material in any medium or format 


**Under the following terms**:
- Attribution — You must give appropriate credit, provide a link to the license, and indicate if changes were made. You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use. 
- NonCommercial — You may not use the material for commercial purposes. 
- NoDerivatives — If you remix, transform, or build upon the material, you may not distribute the modified material. 
- No additional restrictions — You may not apply legal terms or technological measures that legally restrict others from doing anything the license permits. 


----
## Citation
If ***ExSO*** assists you in making the "publicly available" data, actually valuable and accessible, consider citing:

#### APA

  - Natsikas, T. (2025). ExSO: Market Exchange and System Operation analytical framework (Version 1.0.2) [Computer software]. https://github.com/ThanosGkou/exso

#### BibTeX
- @software{Natsikas_ExSO_Market_Exchange_2025,
author = {Natsikas, Thanos},
month = dec,
title = {{ExSO: Market Exchange and System Operation analytical framework}},
url = {https://github.com/ThanosGkou/exso},
version = {1.0.2},
year = {2025}
}


----
