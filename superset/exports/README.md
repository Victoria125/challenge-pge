# Superset dashboard exports

This project previously did not contain any Superset dashboard asset. The
container **cannot recreate the original charts** without an authentic export.
We do not silently substitute or invent the original visualization.

To make the existing dashboard reproducible:

1. Open a Superset instance containing the desired PGE dashboard.
2. Export it (including charts/datasets/databases):
   ```bash
   docker exec arp_superset superset export_dashboards --dashboard-file /tmp/arp-dashboard.zip
   docker cp arp_superset:/tmp/arp-dashboard.zip ./superset/exports/arp-dashboard.zip
   ```
3. Check the archive for hard-coded local URLs, internal secrets, passwords,
   or credentials. Remove secrets before committing any export.
4. Commit `superset/exports/arp-dashboard.zip` to the repository. On future
   `docker compose up` runs, the Superset container will import it using
   `superset import_dashboards`. The bootstrap displays a warning while the
   file is absent.

**Current status:** no authentic exported dashboard ZIP is available in this
repository. The import hook is implemented, but Part 4 is **not yet
reproducible** until the dashboard is exported and checked in. Superset starts
using Gunicorn and no longer enables the Flask reload/debugger flags.
