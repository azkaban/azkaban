.. _GetStartedHead:


Getting Started
==================================

After version 3.0, we provide two modes: the stand alone "solo-server" mode and distributed multiple-executor mode. The following describes the differences between the two modes.

In solo server mode, the DB is embedded H2 and both web server and executor server run in the same process. This should be useful if one just wants to try things out. It can also be used on small scale use cases.

The multiple executor mode is for most serious production environment. Its DB should be backed by MySQL instances with master-slave set up. The web server and executor servers should ideally run in different hosts so that upgrading and maintenance shouldn't affect users. This multiple host setup brings in robust and scalable aspect to Azkaban.

- Set up the database
- Download and install the Web Server
- Configure database to use multiple executors
- Download and install the Executor Server for each executor configured in database
- Install Azkaban Plugins
- Below are instructions on how to set Azkaban up.
