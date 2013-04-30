---
layout: documents
nav: webserversetup
expand: installation
context: ../../..
---
#Setup Azkaban Web Server

Grab the azkaban-web-server package from the [download page](../../../downloads.html).

Alternatively, by cloning the [github repo](https://github.com/azkaban/azkaban2), you can run __ant \-package\-all__ build the latest version
from master.

### Installing the Web Server
Download the _azkaban-web-server_ package (or alternatively build it) and extract it into a directory.
The following directories should be extracted:

<br/>
|Folder      | Description                                                                   |
|------------|-------------------------------------------------------------------------------|
|bin         | The scripts to start Azkaban jetty server                                     |
|conf        | The configurations for Azkaban web server                                     |
|lib         | The jar dependencies for Azkaban                                              |
|extlib      | Additional jars that are added to extlib will be added to Azkaban's classpath |
|plugins     | the directory where plugins can be installed                                  |
|web         | The web (css, javascript, image) files for Azkaban web server.                |

## Under progress...