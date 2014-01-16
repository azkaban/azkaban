---
layout: documents
nav: database
context: ../..
---
#Database Setup

Currently, Azkaban2 only uses MySQL as its data store, although we are evaluating other possible storage systems.

----------
### 1. Install MySQL
Installation of MySQL DB won't be covered by these instructions, but you can access the instructions on 
[MySQL's Document Site](http://dev.mysql.com/doc/index.html).

----------
### 2. Setup the Database
* [Create a database](http://dev.mysql.com/doc/refman/5.7/en/create-database.html) for Azkaban. I.E:

<pre class="code">
# Example database creation command, although the db name doesn't need to be 'azkaban'
mysql> CREATE DATABASE azkaban;
</pre>

* [Create a database user](http://dev.mysql.com/doc/refman/5.7/en/create-user.html) for Azkaban. I.E

<pre class="code">
# Example database creation command. The user name doesn't need to be 'azkaban'
mysql> CREATE USER 'username'@'%' IDENTIFIED BY 'password';
</pre>

* [Set user permission](http://dev.mysql.com/doc/refman/5.7/en/grant.html) on the database. 
Create a user for Azkaban if one hasn't been made, and give the user _INSERT, SELECT, UPDATE, DELETE_ permission on all tables in the Azkaban db.

<pre class="code">
# Replace db, username with the ones created by the previous steps. 
mysql> GRANT SELECT,INSERT,UPDATE,DELETE ON &lt;database&gt;.* to '&lt;username&gt;'@'%' WITH GRANT OPTION;
</pre>

* Configure Packet Size may need to be configured. MySQL may have, by default, a ridiculously low allowable packet size. To increase it, you'll need
to have the property max_allowed_packet set to something higher, say 1024M.
	* To configure this in linux, open _/etc/my.cnf_.
	* Somewhere after mysqld, add the following

<pre class="code">
[mysqld]
...
max_allowed_packet=1024M
</pre>

To restart MySQL, you can run...
<pre class="code">
sudo /sbin/service mysqld restart
</pre>


----------
### 3. Creating the Azkaban Tables

1. Download the azkaban-sql-script tarball from the [download page](../../downloads.html). 
Contained in this archive are table creation scripts.
2. Run individual table creation scripts on the MySQL instance to create your tables. Alternatively, simply run the _create-all-sql_ script.
Any script with '_update_' as a prefix can be ignored.


----------
### 4. Get the JDBC Connector Jar

For various reasons, Azkaban does not distribute the MySQL JDBC connector jar. You can download the jar from this link: [http://www.mysql.com/downloads/connector/j/](http://www.mysql.com/downloads/connector/j/). 
This jar will be needed for both the web server and the executor server and should be dropped into the /extlib directory for both servers.
