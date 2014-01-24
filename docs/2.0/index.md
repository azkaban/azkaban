---
layout: documents
title: "Azkaban 2.0 Documentation"
context: ../..
---

# Installation

Azkaban2 is fairly easy to set up, although has more moving pieces than its predecessor. There are three servers that need to be setup:
* *MySQL instance* - Azkaban uses MySQL to store projects and executions
* *Azkaban Web Server* - Azkaban Web Server is a Jetty server which acts as the controller as well as the web interface
* *Azkaban Executor Server* - Azkaban Executor Server executes submitted workflow.

You can download Azkaban tars from the following link: [Download Packages](downloads.html)

## Setting up the DB
Currently, Azkaban2 only uses MySQL as its data store. Installation of MySQL DB is not covered in this guide. 
1. Download the `azkaban-sql-script` tar. Contained in this archive are table creation scripts.
2. Run the scripts on the MySQL instance to create your tables.

## Getting the JDBC Connector jar
For various licensing reasons, Azkaban does not distribute the MySQL JDBC connector jar. You can download the jar from this link: [http://www.mysql.com/downloads/connector/j/](http://www.mysql.com/downloads/connector/j/). 
This jar will be needed for both the web server and the executor server.

## Setup the Web Server
### Download and Install
1. Download the `azkaban-web-server` tar. Extract it into your azkaban install directory.
2. Copy the jdbc jar into `./extlib` directory. Azkaban will automatically look to this directory for external (non-distributed) jars.

In the conf dir, there should be several files:
* `azkaban.properties` - Used by Azkaban for runtime paramaters
* `global.properties` - Global static properties that are passed as shared properties to every workflow and job.
* `azkaban-users.xml` - Used to add users and roles for authentication. This file is not used if the XmLUserManager is not set up to use it.

### Setting up SSL
Azkaban uses SSL socket connectors, which means a keystore will have to be available. You call follow the steps provided at this link ([http://docs.codehaus.org/display/JETTY/How+to+configure+SSL](http://docs.codehaus.org/display/JETTY/How+to+configure+SSL)) to create one.
Once a keystore file has been created, Azkaban must be given its location and password. Within `azkaban.properties`, the following properties should be overridden.

<pre>
jetty.keystore=keystore
jetty.password=password
jetty.keypassword=password
jetty.truststore=keystore
jetty.trustpassword=password
</pre>

### Setting up the UserManager
Azkaban uses the UserManager to provide authentication and user roles.
By default, Azkaban includes and uses the `XmlUserManager` which gets username/passwords and roles from the `azkaban-users.xml` as can be seen in the `azkaban.properties` file.
* `user.manager.class=azkaban.user.XmlUserManager`
* `user.manager.xml.file=conf/azkaban-users.xml`

The following is an example of the contents of the `azkaban-users.xml` file.

<pre>
&lt;azkaban-users&gt;
  &lt;user username="azkaban" password="azkaban" roles="admin" groups="azkaban"/&gt;
  &lt;role name="admin" permissions="ADMIN" /&gt;
&lt;/azkaban-user&gt;
</pre>

It is possible to override the UserManager to use other methods of authentication (i.e. DB, JNDI, Ldap etc) by including your own implementation of the `azkaban.user.UserManager` interface and changing the `user.manager.class` property.

### Setting up the DB

To point Azkaban web client to the MySQL instance, you will need to configure the following properties in `azkaban.properties`.

<pre>
database.type=mysql
mysql.port=3306
mysql.host=localhost
mysql.database=azkaban2
mysql.user=azkaban
mysql.password=azkaban
mysql.numconnections=100
</pre>

Currently MySQL is the only data store type supported in Azkaban. So `database.type` should always be `mysql`.

### Setting up Web Client

Azkaban allows various property changes. 
