---
layout: documents
nav: gettingstarted
expand: gettingstarted
context: ../..
version: 2.1
---

#Getting Started

Azkaban2 is fairly easy to set up, although has more moving pieces than its predecessor. The most challenging piece to setup is the database.
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

