---
layout: documents
nav: upgradefrom20
context: ../..
---

# Upgrading from 2.0

If installing Azkaban from scratch, you can ignore the rest of this document.

Unfortunately 2.1 needs some other steps before it can be used. Luckily, a minimum amount of downtime is necessary.

1. There are a few new tables, so _create\_sla\_table.sql_ will need to be run.
2. Also, the _update\_2.0\_to\_2.1.sql_ needs to be run to alter all the tables.

The old executor should be able to continue running while the table is being altered.