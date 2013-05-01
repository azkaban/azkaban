---
layout: documents
nav: webserversetup
expand: gettingstarted
context: ../..
---
#Setup Azkaban Web Server
Azkaban Web Server handles project management, authentication, scheduling and trigger of executions.

----------
### Installing the Web Server
Grab the azkaban-web-server package from the [download page](../../downloads.html).

Alternatively, by cloning the [github repo](https://github.com/azkaban/azkaban2), you can run __ant \-package\-all__ build the latest version
from master.

Extract the package into a directory. The install path should be different from the AzkabanExecutorServer.

After extraction, there should be the following directories.

<br/>
|Folder      | Description                                                                   |
|------------|-------------------------------------------------------------------------------|
|bin         | The scripts to start Azkaban jetty server                                     |
|conf        | The configurations for Azkaban web server                                     |
|lib         | The jar dependencies for Azkaban                                              |
|extlib      | Additional jars that are added to extlib will be added to Azkaban's classpath |
|plugins     | the directory where plugins can be installed                                  |
|web         | The web (css, javascript, image) files for Azkaban web server.                |
{.params}


In the _conf_ directory, there should be three files

* **azkaban.properties** - Used by Azkaban for runtime parameters
* **global.properties** - Global static properties that are passed as shared properties to every workflow and job.
* **azkaban-users.xml** - Used to add users and roles for authentication. This file is not used if the XmLUserManager is not set up to use it.

The _azkaban.properties_ file will be the main configuration file that is necessary to setup Azkaban.

----------
### Getting KeyStore for SSL

Azkaban uses SSL socket connectors, which means a keystore will have to be available. You call follow the steps provided at this link ([http://docs.codehaus.org/display/JETTY/How+to+configure+SSL](http://docs.codehaus.org/display/JETTY/How+to+configure+SSL)) to create one.
Once a keystore file has been created, Azkaban must be given its location and password. Within _azkaban.properties_, the following properties should be overridden.
<pre class="code">
jetty.keystore=keystore
jetty.password=password
jetty.keypassword=password
jetty.truststore=keystore
jetty.trustpassword=password
</pre>

----------
### Setting up the DB

If you haven't gotten the MySQL JDBC driver, you can get it from this link: [http://www.mysql.com/downloads/connector/j/](http://www.mysql.com/downloads/connector/j/). 
Drop this jar into the _extlib_ directory. All external dependencies should be added to the extlib directory.

To point Azkaban web client to the MySQL instance, you will need to add the connection parameters to _azkaban.properties_.

<pre class="code">
database.type=mysql
mysql.port=3306
mysql.host=localhost
mysql.database=azkaban
mysql.user=azkaban
mysql.password=azkaban
mysql.numconnections=100
</pre>

Currently MySQL is the only data store type supported in Azkaban. So _database.type_ should always be _mysql_.

----------
### Setting up the UserManager
Azkaban uses the UserManager to provide authentication and user roles.
By default, Azkaban includes and uses the _XmlUserManager_ which gets username/passwords and roles from the _azkaban-users.xml_ as can be seen in the _azkaban.properties_ file.
* `user.manager.class=azkaban.user.XmlUserManager`
* `user.manager.xml.file=conf/azkaban-users.xml`

----------
### Running Web Server
The following properties in _azkaban.properties_ are used to configure jetty.
<pre class="code">
jetty.maxThreads=25
jetty.ssl.port=8443
</pre>

Execute _bin/azkaban-web-start.sh_ to start AzkabanWebServer. 
To shutdown the AzkabanWebServer, run _bin/azkaban-web-shutdown.sh_

You can test access by accessing the web server through a browser.

