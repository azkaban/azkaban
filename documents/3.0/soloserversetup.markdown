---
layout: documents
nav: soloserversetup
context: ../..
---
#Setup Azkaban Solo Server

In 3.0 there is a solo server mode for one to try out Azkaban or use in small scale and less security environment.
Its features are:

* easy to install

No MySQL instance is needed. It packages H2 as its main persistence storage.

* easy to start up

Both web server and executor server run in the same process.

* full feature

It packages all Azkaban features. You can use it in normal ways and install plugins for it.


----------
### Installing the Solo Server
Grab the azkaban-solo-server package from the [download page](../../downloads.html).

Alternatively, by cloning the [github repo](https://github.com/azkaban/azkaban2), you can run __ant \-package\-all__ build the 3.0 version
from branch release-3.0.

Extract the package into a directory. 

After extraction, there should be the following directories.

<br/>
|Folder      | Description                                                                   |
|------------|-------------------------------------------------------------------------------|
|bin         | The scripts to start Azkaban jetty server                                     |
|conf        | The configurations for Azkaban solo server                                     |
|lib         | The jar dependencies for Azkaban                                              |
|extlib      | Additional jars that are added to extlib will be added to Azkaban's classpath |
|plugins     | the directory where plugins can be installed                                  |
|web         | The web (css, javascript, image) files for Azkaban web server.                |
{.params}


In the _conf_ directory, there should be three files

* **azkaban.private.properties** - Used by Azkaban for runtime parameters
* **azkaban.properties** - Used by Azkaban for runtime parameters
* **global.properties** - Global static properties that are passed as shared properties to every workflow and job.
* **azkaban-users.xml** - Used to add users and roles for authentication. This file is not used if the XmLUserManager is not set up to use it.

The _azkaban.properties_ file will be the main configuration file

----------
### Getting KeyStore for SSL (Optional)

Azkaban solo server by default doesn't use SSL. But you could set it up the same way in a stand alone web server. Here is how:

Azkaban web server could use SSL socket connectors, which means a keystore will have to be available. You call follow the steps provided at this link ([http://docs.codehaus.org/display/JETTY/How+to+configure+SSL](http://docs.codehaus.org/display/JETTY/How+to+configure+SSL)) to create one.
Once a keystore file has been created, Azkaban must be given its location and password. Within _azkaban.properties_, the following properties should be overridden.
<pre class="code">
jetty.keystore=keystore
jetty.password=password
jetty.keypassword=password
jetty.truststore=keystore
jetty.trustpassword=password
</pre>


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
jetty.ssl.port=8081
</pre>

Execute _bin/azkaban-solo-start.sh_ to start the solo server. 
To shutdown, run _bin/azkaban-solo-shutdown.sh_

Open [http://localhost:8081/index](http://localhost:8081/index) on your browser.

