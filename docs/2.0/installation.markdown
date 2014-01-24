---
layout: test
nav: installation
expand: documents
context: ../..
version: 2.0
---

#Installation

Azkaban2 is fairly easy to set up, although has more moving pieces than its predecessor. There are three servers that need to be setup:
* **MySQL instance** - Azkaban uses MySQL to store projects and executions
* **Azkaban Web Server** - Azkaban Web Server is a Jetty server which acts as the controller as well as the web interface
* **Azkaban Executor Server** - Azkaban Executor Server executes submitted workflow.

You can download Azkaban tars from the following link: [Download Packages](downloads.html)

## Setting up the DB
Currently, Azkaban2 only uses MySQL as its data store. Installation of MySQL DB is not covered in this guide. 
1. Download the _azkaban-sql-script_ tar. Contained in this archive are table creation scripts.
2. Run the scripts on the MySQL instance to create your tables.

## Getting the JDBC Connector jar
For various licensing reasons, Azkaban does not distribute the MySQL JDBC connector jar. You can download the jar from this link: [http://www.mysql.com/downloads/connector/j/](http://www.mysql.com/downloads/connector/j/). 
This jar will be needed for both the web server and the executor server.

## Setup the Web Server
### Download and Install
1. Download the _azkaban-web-server_ tar. Extract it into your azkaban install directory.
2. Copy the jdbc jar into _./extlib_ directory. Azkaban will automatically look to this directory for external (non-distributed) jars.

In the conf dir, there should be several files:
* **azkaban.properties** - Used by Azkaban for runtime paramaters
* **global.properties** - Global static properties that are passed as shared properties to every workflow and job.
* **azkaban-users.xml** - Used to add users and roles for authentication. This file is not used if the XmLUserManager is not set up to use it.

### Setting up SSL
Azkaban uses SSL socket connectors, which means a keystore will have to be available. You call follow the steps provided at this link ([http://docs.codehaus.org/display/JETTY/How+to+configure+SSL](http://docs.codehaus.org/display/JETTY/How+to+configure+SSL)) to create one.
Once a keystore file has been created, Azkaban must be given its location and password. Within _azkaban.properties_, the following properties should be overridden.
````
    jetty.keystore=keystore
    jetty.password=password
    jetty.keypassword=password
    jetty.truststore=keystore
    jetty.trustpassword=password
````

### Setting up the UserManager
Azkaban uses the UserManager to provide authentication and user roles.
By default, Azkaban includes and uses the _XmlUserManager_ which gets username/passwords and roles from the _azkaban-users.xml_ as can be seen in the _azkaban.properties_ file.
* `user.manager.class=azkaban.user.XmlUserManager`
* `user.manager.xml.file=conf/azkaban-users.xml`

The following is an example of the contents of the _azkaban-users.xml_ file.
````
	<azkaban-users>    
		<user username="azkaban" password="azkaban" roles="admin" groups="azkaban"/>    
		<role name="admin" permissions="ADMIN" />    
	</azkaban-user>    
````
It is possible to override the UserManager to use other methods of authentication (i.e. DB, JNDI, Ldap etc) by including your own implementation of the _azkaban.user.UserManager_ interface and changing the _user.manager.class_ property.

### Setting up the DB
To point Azkaban web client to the MySQL instance, you will need to configure the following properties in _azkaban.properties_.

````
    database.type=mysql
    mysql.port=3306
    mysql.host=localhost
    mysql.database=azkaban2
    mysql.user=azkaban
    mysql.password=azkaban
    mysql.numconnections=100
````
Currently MySQL is the only data store type supported in Azkaban. So _database.type_ should always be _mysql_.

### Setting up Web Client
Azkaban allows various property changes. 