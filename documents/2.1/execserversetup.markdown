---
layout: documents
nav: execserversetup
expand: gettingstarted
context: ../..

---
#Setup Azkaban Executor Server
Azkaban Executor Server handles the actual execution of the workflow and jobs.

----------
### Installing the Executor Server
Grab the azkaban-exec-server package from the [download page](../../downloads.html).

Alternatively, by cloning the [github repo](https://github.com/azkaban/azkaban2), you can run __ant \-package\-all__ build the latest version
from master.

Extract the package into a directory. The install path should be different from the Azkaban Web Server.

After extraction, there should be the following directories.

<br/>
|Folder      | Description                                                                   |
|------------|-------------------------------------------------------------------------------|
|bin         | The scripts to start Azkaban jetty server                                     |
|conf        | The configurations for Azkaban exec server                                     |
|lib         | The jar dependencies for Azkaban                                              |
|extlib      | Additional jars that are added to extlib will be added to Azkaban's classpath |
|plugins     | the directory where plugins can be installed                                  |
{.params}

In the _conf_ directory, we only need to configure the _azkaban.properties_ file. 
This file is the be the main configuration file that is necessary to setup Azkaban Executor.

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
### Configuring AzabanWebServer and AzkabanExecutorServer clients
The Executor server needs to be setup with a port, and the AzabanWebServer will need to know what this port is.
The following properties need to be set on AzkabanExecutorServer's _azkaban.properties_.

<pre class="code">
# Azkaban Executor settings
executor.maxThreads=50
executor.port=12321
executor.flow.threads=30
</pre>

By default the _executor.port_ is set to 12321. The AzkabanWebServer will have to point to this port as well.
This is done by setting the following property in AzkabanWebServer's _azkaban.properties_.
<pre class="code">
executor.port=12321
</pre>

These changes are only picked up after restarting the servers.
### Running Executor Server
Execute _bin/azkaban-exec-start.sh_ to start AzkabanExecutorServer. 
To shutdown the AzkabanExecutorServer, run _bin/azkaban-exec-shutdown.sh_



