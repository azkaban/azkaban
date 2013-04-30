---
layout: documents
nav: database
expand: installation
context: ../../..
---
#Database Setup

Azkaban currently requires a MySQL instance to be run.

Installation of MySQL DB won't be covered by these instructions, but you can access the instructions on 
[MySQL's Document Site](http://dev.mysql.com/doc/index.html).


Currently, Azkaban2 only uses MySQL as its data store. Installation of MySQL DB is not covered in this guide. 
1. Download the _azkaban-sql-script_ tar. Contained in this archive are table creation scripts.
2. Run the scripts on the MySQL instance to create your tables.

## Getting the JDBC Connector jar
For various licensing reasons, Azkaban does not distribute the MySQL JDBC connector jar. You can download the jar from this link: [http://www.mysql.com/downloads/connector/j/](http://www.mysql.com/downloads/connector/j/). 
This jar will be needed for both the web server and the executor server.

## Under progress...