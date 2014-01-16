---
layout: documents
nav: azkabanexecserver
context: ../..
---
#Azkaban Executor Server Configurations

### Executor Server Properties

|{.parameter}Parameter|{.description} Description                           |{.default}Default    |
|---------------------|-----------------------------------------------------|---------------------|
|executor.port        | The port for azkaban executor server                | 12321               |
|executor.global.properties | A path to the properties that will be the parent for all jobs. | _none_ |
|azkaban.execution.dir| The folder for executing working directories        | executions          |
|azkaban.project.dir  | The folder for storing temporary copies of project files used for executions | projects     |
|executor.flow.threads| The number of simulateous flows that can be run. These threads are mostly idle. |30|
|job.log.chunk.size   | For rolling job logs. The chuck size for each roll over | 5MB |
|job.log.backup.index | The number of log chunks. The max size of each logs is then the index * chunksize | 4 |
|flow.num.job.threads | The number of concurrent running jobs in each flow. These threads are mostly idle. | 10 | 
{.params}

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
