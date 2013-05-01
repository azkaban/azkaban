---
layout: documents
nav: azkabanexecserver
expand: configuration
context: ../..
---
#Azkaban Executor Server Configurations

### MySQL Connection Parameter
|{.parameter}Parameter|{.description} Description                                                  |{.default}Default    |
|---------------------|----------------------------------------------------------------------------|---------------------|
|database.type        | The database type. Currently, the only database supported is mysql.        | mysql               |
|mysql.port           | The port to the mysql db                                                   | 3306                |
|mysql.host           | The mysql host                                                             | localhost           |
|mysql.database       | The mysql database                                                         |                     |
|mysql.user           | The mysql user                                                             |                     |
|mysql.password       | The mysql password                                                         |                     |
|mysql.numconnections | The number of connections that Azkaban web client can open to the database | 100                 |
{.params}

### Executor Server Properties
|{.parameter}Parameter|{.description} Description                           |{.default}Default    |
|---------------------|-----------------------------------------------------|---------------------|
|executor.host        | The host for azkaban executor server                | localhost           |
{.params}