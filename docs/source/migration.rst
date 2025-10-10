.. _migration:

In general
==========
- Any new exso version may introduce changes to some reports, that require a **retrospective implementation**.
- Depending on, which version of exso you're migrating from, this wiping out and remake may not be necessary
- For simplicity, version 0.0.0 is assumed as the previous version used.
- General guideline: with every new exso version, it's safest rebuild the whole database (default option integrated in exso as-packaged)
- Remember to re-set system formats (if needed) in exso (exso.settings.set_system_formats()) after each new version is installed.


Data readiness for |version| (15min products)
=============================================
In order for the **data** migration process to be smooth, at least one of the following must be true:
    1. It's the first time you are using exso
    2. You completely delete your local old "database" folder
    3. You haven't attempted to use (update mode) any previous exso-version after 30/09/2025.
    4. (advanced/not-recommended) You did use a previous exso-version after 30/09/2025, but made sure that you (manually?) delete ANY row in ANY affected file that refers to UTC-datetimes past 30/09/2025 23:00 [or 23:30] CET (or, 30/09/2025 21:00 [or 21:30] UTC)

Modification of to-refurbish reports (advanced/not-recommended)
================================================================
**IF**

- You are migrating from exso **v1.0+**,
- You are **certain** that you fall in **option 3**, or choose **option 4**

**THEN**

- You can avoid the full remake of affected reports, by running this line of code just once:

    :code:`exso.settings.set_refresh_requirements(force_no_refresh = 'all', mode = 'w')`
- Or, to exclude specific reports:
    :code:`exso.settings.set_refresh_requirements(force_no_refresh = List)`

An intermediate, safe and easy approach if coming from exso v1.0+ (NOT from v0.0), you can force_no_refresh the following reports:

IDM_CRIDA1_AggDemandSupplyCurves, IDM_CRIDA2_AggDemandSupplyCurves, IDM_CRIDA3_AggDemandSupplyCurves,
IDM_LIDA1_AggDemandSupplyCurves, IDM_LIDA2_AggDemandSupplyCurves, IDM_LIDA3_AggDemandSupplyCurves,
IDM_CRIDA1_MarketCoupling, IDM_CRIDA2_MarketCoupling, IDM_CRIDA3_MarketCoupling
IDM_LIDA1_MarketCoupling, IDM_LIDA2_MarketCoupling, IDM_LIDA3_MarketCoupling
IDM_CRIDA1_Results, IDM_CRIDA2_Results, IDM_CRIDA3_Results
IDM_LIDA1_Results, IDM_LIDA2_Results, IDM_LIDA3_Results
IDM_CRIDA1_ResultsSummary, IDM_CRIDA2_ResultsSummary, IDM_CRIDA3_ResultsSummary
IDM_LIDA1_ResultsSummary, IDM_LIDA2_ResultsSummary, IDM_LIDA3_ResultsSummary
DailyAuctionsSpecificationsATC, DailyDispatchOfCreteElectricalSystem
DayAheadLoadForecast, DayAheadRESForecast, DayAheadSchedulingUnitAvailabilities, HVCUSTCONS,
DAS

Code::

    text = '''
    IDM_CRIDA1_AggDemandSupplyCurves, IDM_CRIDA2_AggDemandSupplyCurves, IDM_CRIDA3_AggDemandSupplyCurves,
    IDM_LIDA1_AggDemandSupplyCurves, IDM_LIDA2_AggDemandSupplyCurves, IDM_LIDA3_AggDemandSupplyCurves,
    IDM_CRIDA1_MarketCoupling, IDM_CRIDA2_MarketCoupling, IDM_CRIDA3_MarketCoupling
    IDM_LIDA1_MarketCoupling, IDM_LIDA2_MarketCoupling, IDM_LIDA3_MarketCoupling
    IDM_CRIDA1_Results, IDM_CRIDA2_Results, IDM_CRIDA3_Results
    IDM_LIDA1_Results, IDM_LIDA2_Results, IDM_LIDA3_Results
    IDM_CRIDA1_ResultsSummary, IDM_CRIDA2_ResultsSummary, IDM_CRIDA3_ResultsSummary
    IDM_LIDA1_ResultsSummary, IDM_LIDA2_ResultsSummary, IDM_LIDA3_ResultsSummary
    DailyAuctionsSpecificationsATC, DailyDispatchOfCreteElectricalSystem
    DayAheadLoadForecast, DayAheadRESForecast, DayAheadSchedulingUnitAvailabilities, HVCUSTCONS,
    DAS
    '''

    import re
    force_no_refresh = [r.strip() for r in re.sub('\n','',text).split(',')]
    exso.settings.set_refresh_requirements(force_no_refresh = force_no_refresh, mode = 'a')




If anything goes wrong
===========================
- Deleting the whole database folder (or just the report that's going wrong) should work




