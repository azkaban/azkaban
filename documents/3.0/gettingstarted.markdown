---
layout: documents
nav: gettingstarted
context: ../..
version: 3.0
---

#Getting Started

In version 3.0 we provide two modes: the stand alone "solo-server" mode and the heavier weight two server mode.
Here are the differences between the two modes:

* In solo server mode, the DB is embedded H2 and both web server and executor server run in the same process. This should be useful if one just wants to try things out. It can also be used on small scale use cases.

* The two server mode is for more serious production environment. Its DB should be backed by MySQL instances with master-slave set up. The web server and executor server should run in different processes so that upgrading and maintenance shouldn't affect users.

Below are instructions on how to set Azkaban up.

## Solo Server Mode:

### 1. [Download and Install Solo Server Package](./soloserversetup.html)
* Download the solo server package for all you need to start Azkaban.

### 2. [Install Azkaban Plugins](./pluginsetup.html)
* Optional step for installing common Azkaban plugins.

## Two Server Mode:

Azkaban2 two server mode is fairly easy to set up, although it has more moving pieces than its predecessor. The most challenging piece to setup is the database.
There are three servers that need to be setup:
* **MySQL instance** - Azkaban uses MySQL to store projects and executions
* **Azkaban Web Server** - Azkaban Web Server is a Jetty server which acts as the controller as well as the web interface
* **Azkaban Executor Server** - Azkaban Executor Server executes submitted workflow.

### 1. [Setup the Database](./database.html)
* Azkaban uses MySQL to manage projects, schedules and executions.

### 2. [Download and Install the Web Server](./webserversetup.html)
* Azkaban Web server consists of the web interface, the scheduler and the project manager

### 3. [Download and Install the Executor Server](./execserversetup.html)
* Azkaban Executor server runs the workflows

### 4. [Install Azkaban Plugins](./pluginsetup.html)
* Optional step for installing common Azkaban plugins.

