
![museo_moderno.png](docs/source/figs/logos/museo_moderno.png)

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
## What's New

Checkout new features [here](https://exso.readthedocs.io/en/latest/whats_new.html)

-----
## Documentation

ExSO documentation is available in [ReadTheDocs](https://exso.readthedocs.io/en/latest/)

-----

## Visualization
***exso*** utilizes the (extremely helpful and interactive) package [plotly](https://plotly.com/python) for data visualization.
The visualization of a Node object is as simple as calling its .plot() method:

Graphs can be zoomed in/out, rescaled, columns can be toggled-on/off in real time.

&rarr; By default, ***exso*** will **omit to plot any columns that are Zero or NaN** over the whole selected timerange, in order to make the plot lighter, both compuatationally and on the eyes.

```sh
isp1_thermal_gen = t['root.admie.isp1ispresults.isp_schedule.thermal']
fig = isp1_thermal_gen.plot(area = True, start_date = '2022-1-1', end_date = '2022-1-10', tz = 'EET', show = True, save_path = None)

# by default:  tz = 'EET', start_date = None, end_date = None,  area = False, show = True, save_path = Non

# the returned figure is of type "plotly.graph_objs._figure.Figure", meaning, you can set "show"=False, and update the layout with normal plotly usage before displaying it.
# Some very basic modification-options (title, x&y labels) will be supported directly through the exso.Node object
```
![plotly_viz.png](resources/plotly_viz.png)

----
## Features under active Development
### Data Documentation
Another aspect that creates difficulties in utilizing the published data (after one overcomes the sparsity of data), is the lack of detailed documentation per report, field, or property.
(e.g. The term "Net Load" may mean System Load minus pumping load, or Consumption minus RES, or Consumption minus RES minus pumping load, etc.)
At this stage, the Data Documentation provided in the ***exso*** package is far from perfect: Data Documentation is currently only on the report-level, providing high-level insights but not detailed disambiguations.

- A custom-made documentation, built as a light non-relational database is currently being developed and will be launched with one of the next versions of ***exso***.

### Analytics API
The current setup is oriented around *reports*. An Analytics API currently under development, will facilitate:
- Seaming properties from different reports of different timeframws (e.g. System Marginal Price to Market Clearing Price)
- Dedicated reporting and visualization (e.g. Daily System Snapshot of market prices, imports, loads, reserves, balancing, generation mix, etc.)
- Advanced analytics methods (e.g. Unit Unavailabilities statistics, comparisons, correlations)

### Support for more Reports
One of the following version, will contain some improvements on existing reports, and the addition of Water declaration and NTC reports.

Also, **ENTSO-E** will be added to the **supported publishers**, for across-europe data collection, integrated in the same local database.

### Support for Linux
Support for Linux-based systems is not foreseen at the moment, but feel free to submit a request if needed.

----
## Tests
- ***ExSO*** is fairly tested for the envisaged usage, but since the project is not (at least yet) intended for collaborative development, tests are not published.

- The design philosophy is not to catch all errors imagineable, but rather that basic users will stick to basic/documented usage, and that advanced users know what they're doing
- From the user's perspective, the [Validation module](#data-validation) is available to assist in validating/trusting that the database accurately reflects the raw datalaek files. 
----
## Issues
- Feel free to submit any issues [here](https://github.com/ThanosGkou/exso/issues) or via e-mail
- Use of ***ExSO*** (update mode) with conda environments, was reported to present encoding issues during printing coloured text in the console, and despite otherwise being functional, the print statement after each report's success causes an encoding error. Try to use pip environments instead.



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

  - Natsikas, T. (2024). ExSO: Market Exchange and System Operation analytical framework (Version 1.0.0) [Computer software]. https://github.com/ThanosGkou/exso

#### BibTeX
- @software{Natsikas_ExSO_Market_Exchange_2024,
author = {Natsikas, Thanos},
month = apr,
title = {{ExSO: Market Exchange and System Operation analytical framework}},
url = {https://github.com/ThanosGkou/exso},
version = {1.0.0},
year = {2024}
}


----
