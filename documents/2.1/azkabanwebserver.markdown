---
layout: documents
nav: azkabanwebserver
context: ../..
---
#Azkaban Web Server Configurations
These are properties to configure the web server. They can be set in _azkaban.properties_.

### General Properties
|{.parameter}Parameter|{.description} Description                                                                                       |{.default}Default    |
|---------------------|-----------------------------------------------------------------------------------------------------------------|---------------------|
|azkaban.name         | The name of the azkaban instance that will show up in the UI. Useful if you run more than one Azkaban instance. | Local               |
|azkaban.label        | A label to describe the Azkaban instance.                                                                       | My Local Azkaban    |
|azkaban.color        | Hex value that allows you to set a style color for the Azkaban UI.                                              | #FF3601             |
|web.resource.dir     | Sets the directory for the ui’s css and javascript files.                                                       | web/                |
|default.timezone     | The timezone that will be displayed by Azkaban.                                                                 | America/Los_Angeles |
|viewer.plugin.dir    | Directory where viewer plugins are installed.                                                                   | plugins/viewer      |
{.params}

### Jetty Parameter
|{.parameter}Parameter|{.description} Description                           |{.default}Default    |
|---------------------|-----------------------------------------------------|---------------------|
|jetty.maxThreads     | Max request threads                                 | 25                  |
|jetty.ssl.port       | The ssl port                                        | 8443                |
|jetty.keystore       | The keystore file                                   |                     |
|jetty.password       | The jetty password                                  |                     |
|jetty.keypassword    | The keypassword                                     |                     |
|jetty.truststore     | The trust store                                     |                     |
|jetty.trustpassword  | The trust password                                  |                     |
{.params}

### Project Manager Settings
|{.parameter}Parameter               |{.description} Description                                                       |{.default}Default    |
|------------------------------------|---------------------------------------------------------------------------------|---------------------|
|project.temp.dir                    | The temporary directory used when uploading projects                            | temp                |
|project.version.retention           | The number of unused project versions retained before cleaning                  | 3                   |
|creator.default.proxy               | Auto add the creator of the projects as a proxy user to the project.            | true                |
|lockdown.create.projects            | Prevents anyone except those with Admin roles to create new projects.           | false               |
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

### Executor Manager Properties
|{.parameter}Parameter|{.description} Description                           |{.default}Default    |
|---------------------|-----------------------------------------------------|---------------------|
|executor.port        | The port for the azkaban executor server            | 12321               |
|executor.host        | The host for azkaban executor server                | localhost           |
|execution.logs.retention.ms | Time in milliseconds that execution logs are retained | 7257600000L (12 weeks) |
{.params}

### Notification Email Properties
|{.parameter}Parameter|{.description} Description                           |{.default}Default    |
|---------------------|-----------------------------------------------------|---------------------|
|mail.sender          | The email address that azkaban uses to send emails. |                     |
|mail.host            | The email server host machine.                      |                     |
|mail.user            | The email server user name.                         |                     |
|mail.password        | The email password user name.                       |                     |
{.params}

### User Manager Properties
|{.parameter}Parameter |{.description} Description                                                                                                                                                  |{.default}Default                     |
|----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------|
|user.manager.class    | The user manager that is used to authenticate a user. The default is an XML user manager, but it can be overwritten to support other authentication methods, such as JDNI. | azkaban.user.XmlUserManager |
|user.manager.xml.file | Xml file for the XmlUserManager                                                                                                                                            | conf/azkaban-users.xml      |
{.params}

### User Session Properties
|{.parameter}Parameter|{.description} Description                                 |{.default}Default|
|---------------------|-----------------------------------------------------------|-----------------|
|session.time.to.live | The session time to live in ms seconds                    | 86400000        |
|max.num.sessions     | The maximum number of sessions before people are evicted. |  10000          |
{.params}
