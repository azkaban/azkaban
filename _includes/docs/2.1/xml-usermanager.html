---
layout: documents
nav: xmlusermanager
context: ../..
---

#XmlUserManager

The XmlUserManager is the default UserManager that is built into Azkaban.
To explicitly set the parameters that configure the XmlUserManager, the following parameters
can be set in the _azkaban.properties_ file.

|{.parameter}Parameter |{.default}Default               |
|----------------------|--------------------------------|
|user.manager.class    | azkaban.user.XmlUserManager    |
|user.manager.xml.file | azkaban-users.xml              |
{.params}

The other file that needs to be modified is the _azkaban-users.xml_ file. The XmlUserManager will
parse the user xml file once during startup to set up the users.

Everything must be enclosed in a _azkaban-users_ tag.
<pre class="code">
&lt;azkaban-users&gt;
	...
&lt;/azkaban-users&gt;
</pre>

<hr/>

### Users

To add users, add the _user_ tag.

<pre class="code">
&lt;azkaban-users&gt;
  &lt;user username="myusername" password="mypassword" roles="a" groups="mygroup" / &gt;
  &lt;user username="myusername2" password="mypassword2" roles="a, b" groups="ga, gb" / &gt;
  ...
&lt;/azkaban-users&gt;
</pre>

|{.parameter}Attributes|{.default}Values                                             |{.default}Required? |
|----------------------|-------------------------------------------------------------|--------------------|
|username              | The login username.                                         | yes                |
|password              | The login password.                                         | yes                |
|roles                 | Comma delimited list of roles that this user has.         | no                 |
|groups                | Comma delimited list of groups that the users belongs to. | no                 |
|proxy                 | Comma delimited list of proxy users that this users can give to a project | no |
{.params}


<hr/>

###Groups

To define each group, you can add the _group_ tag.
<pre class="code">
&lt;azkaban-users&gt;
  &lt;user username="a" ... groups="groupa" / &gt;
  ...
  &lt;group name="groupa" roles="myrole" / &gt;
  ...
&lt;/azkaban-users&gt;
</pre>

In the previous example, user 'a' is in the group 'groupa'. User 'a' would also have the 'myrole' role.
A regular user cannot add group permissions to a project unless they are members of that group.

The following are some group attributes that you can assign.

|{.parameter}Attributes|{.default}Values                                             |{.default}Required? |
|----------------------|-------------------------------------------------------------|--------------------|
|name                  | The group name                                              | yes                |
|roles                 | Comma delimited list of roles that this user has.           | no                 |
{.params}

<hr/>

### Roles
Roles are different in that it assigns global permissions to users in Azkaban. You can set up roles with the
_roles_ tag.

<pre class="code">
&lt;azkaban-users&gt;
  &lt;user username="a" ... groups="groupa" roles="readall" / &gt;
  &lt;user username="b" ... / &gt;
  ...
  &lt;group name="groupa" roles="admin" / &gt;
  ...
  &lt;role name="admin" permissions="ADMIN" / &gt;
  &lt;role name="readall" permissions="READ" / &gt;
&lt;/azkaban-users&gt;
</pre>

In the above example, user 'a' has the role 'readall', which is defined as having the READ permission.
This means that user 'a' has global READ access on all the projects and executions.
User 'a' also is in 'groupa', which has the role ADMIN. It's certainly redundant, but user 'a' is also granted the ADMIN role on all projects.

The following are some group attributes that you can assign.

|{.parameter}Attributes|{.default}Values                                             |{.default}Required? |
|----------------------|-------------------------------------------------------------|--------------------|
|name                  | The group name                                              | yes                |
|permissions           | Comma delimited list global permissions for the role        | yes                 |
{.params}

The possible role permissions are the following:

|{.parameter}Permissions|{.default}Values                                              |
|-----------------------|--------------------------------------------------------------|
|ADMIN                  | Grants all access to everything in Azkaban.                  |
|READ                   | Gives users read only access to every project and their logs |
|WRITE                  | Allows users to upload files, change job properties or remove any project |
|EXECUTE                | Allows users to trigger the execution of any flow            |
|SCHEDULE               | Users can add or remove schedules for any flows              |
|CREATEPROJECTS         | Allows users to create new projects if project creation is locked down |
{.params}
