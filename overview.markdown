---
layout: azkaban
nav: overview
context: ../..
version: 2.1
---

#Overview

Azkaban was implemented at LinkedIn to solve the problem of Hadoop job dependencies. We had jobs that needed to run in order,
from etl jobs to data analytics products.
<br/>
<br/>

Initially a single server solution, with the increased number of Hadoop users over the years, Azkaban has evolved to be a more robust solution.
<br/>
<br/>

Azkaban consists of 3 key components:
* Relational Database (MySQL)
* AzkabanWebServer
* AzkabanExecutorServer

<img title="Azkaban Overview" src="images/azkaban2overviewdesign.png" ALT="Azkaban Overview" width="500" />

<br/>
<br/>
## Relational Database (MySQL)
Azkaban uses MySQL to store much of its state. Both the AzkabanWebServer and the AzkabanExecutorServer access the DB.

#### How does AzkabanWebServer use the DB?
The web server uses the db for the following reasons:
* __Project Management__ - The projects, the permissions on the projects as well as the uploaded files.
* __Executing Flow State__ - Keep track of executing flows and which Executor is running them.
* __Previous Flow/Jobs__ - Search through previous executions of jobs and flows as well as access their log files.
* __Scheduler__ - Keeps the state of the scheduled jobs.
* __SLA__ - Keeps all the sla rules
<br/>
<br/>

#### How does the AzkabanExecutorServer use the DB?
The executor server uses the db for the following reasons:
* __Access the project__ - Retrieves project files from the db.
* __Executing Flows/Jobs__ - Retrieves and updates data for flows and that are executing
* __Logs__ - Stores the output logs for jobs and flows into the db.
* __Interflow dependency__ - If a flow is running on a different executor, it will take state from the DB.

There is no reason why MySQL was chosen except that it is a widely used DB. We are looking to implement compatibility with other DB's, 
although the search requirement on historically running jobs benefits from a relational data store.

<br/>
<br/>
## AzkabanWebServer
The AzkabanWebServer is the main manager to all of Azkaban. It handles project management, authentication, scheduler, and monitoring of executions.
It also serves as the web user interface.

<br/>
<br/>
## AzkabanExecutorServer
Previous versions of Azkaban had both the AzkabanWebServer and the AzkabanExecutorServer features in a single server. The Executor has
since been separated into its own server. There were several reasons for splitting these services: we will soon be able to scale the number 
of executions and fall back on operating Executors if one fails. Also, we are able to roll our upgrades of Azkaban with minimal impact on the users. 
As Azkaban's usage grew, we found that upgrading Azkaban became increasingly more difficult as all times of the day became 'peak'.
 




