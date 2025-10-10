.. _migration:

In general
==========
- Any new exso version may introduce changes to some reports, that require a **retrospective implementation**.
- Depending on, which version of exso you're migrating from, this wiping out and remake may not be necessary
- For simplicity, version 0.0.0 is assumed as the previous version used.


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





If anything goes wrong
===========================
- Deleting the whole database folder (or just the report that's going wrong) should work




