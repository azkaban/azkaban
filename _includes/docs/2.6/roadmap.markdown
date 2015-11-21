---
layout: documents
nav: roadmap
context: ../..
version: 3.0
---

# Roadmap

Here is the roadmap/wishlist we want for Azkaban:

* Azkaban Admin Server
**Idea**: We want Azkaban installation/maintenance/monitoring easier, with a UI. This will also serve as a basis for Azkaban to grow beyond two servers, with a ton of plugins.
**Design**: There should be an Admin Server which can a) setup, config and start up Azkaban servers, b) perform maintenance tasks such as shutdown/restart servers, c) providing monitoring of Azkaban servers and flows/jobs running on these servers, and d) manage Azkaban HA.
**Status**: Work-In-Progress.

* Azkaban Persistence Server/Layer
**Idea**: The move from disk based persistence to DB based persistence was a big win. We no longer suffer Azkaban re-setup due to disk failures. But over the time, persisting everything into MySql becomes a problem. The heavy items such as project files and job logs burden the MySql machines, which in term threatens Azkaban operations. We also want Azkaban to survive temporarily when MySql is down. Therefore, we want to have dedicated persistence layer that could support different persistence options.
**Design**: There should be an Persistence Server or Layer which implement the loaders. It should be able to persist onto different plug-able persistence median. It should be able to use local disk for temporary storage when remote persistence is down. It should monitor remote persistence and alert if it is down.
**Status**: Not-Started-Yet.

* Decoupling UI from rest of Azkaban
**Idea**: Azkaban UI has been the biggest pain in its development. We really should separate UI from the rest of Azkaban so that web UI development can be decoupled and some dedicated UI person can take care of it.
**Design**: Need to carve out the UI from web server and make it standard with web UI design.
**Status**: Not-Started-Yet.

* Explicit Trigger System
**Idea**: In 3.0 we introduced the generic trigger system. Over time we want to design it to be more explicit and more visible to users. We also want to make it into flow control and add trigger nodes into flows.
**Design**: NA.
**Status**: Not-Started-Yet.

* Plugin Flow Control
**Idea**: We have introduced a number of plugins into Azkaban. This has greatly extended Azkaban's functionality. Flow control, however, remain to be a single giant piece and it has become too complicated. We want this part to be redesigned and be plugin-able so that it doesn't get over complicated and people can swap in their own flow control logic.
**Design**: Should redesign the flow control part and make it plugin.
**Status**: Not-Started-Yet.

* Improved Statistics / Visualization
**Idea**: Azkaban has a lot of data about user flows and past executions. We want to make use of these data and make good statistics and visualization out of it.
**Design**: Mostly re-make of UI and add statistics.
**Status**: Work-In-Progress.

* Kerberos for Azkaban
**Idea**: When deploying for Hadoop environment, it make sense for Azkaban to use the same authentication mechanism such as Kerberos.
**Design**: NA.
**Status**: Not-Started-Yet.

* Project Management
**Idea**: Azkaban provides isolated project spaces for user projects. We want to augment that part and make it work with some of the existing tools so that Azkaban can be better integrated with other tools. The goals are to a) allow project update via git repo or hudson or other sources automatically, b) provide project file browser, and c) allow upload diffs rather than full package.
**Design**: NA.
**Status**: Not-Started-Yet.