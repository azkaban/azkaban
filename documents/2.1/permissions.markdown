---
layout: documents
nav: permissions
expand: permissions
context: ../..
---

#Permissions
When you start Azkaban, you may notice the login page. Azkaban makes you authenticate before you can use it. This is prevent seeing or executing workflows you shoudn't see or touch.
We also used authenticated users for auditing purposes. Whenever project files change, is modified, scheduled, etc. we often want to
know which user performed that action.

<img class="shadowimg" title="Azkaban Login" src="./images/login.png" ALT="Azkaban Login" width="400" />

## UserManager

All of the authentication occurs in the UserManager. The UserManager determines validates users, group ownership and roles. 
The [XmlUserManager](./xmlusermanager.html) is the default UserManager.

If you're trying to integrate Azkaban with an already established authentication mechanism,
 it is easy to create your own [Custom UserManager](./customusermanager.html) (i.e. an LDAPUserManager).

## Project Permissions

A project owner can control which user can perform certain actions on a project.
User permissions can only be added by 'admins' of a project, or those with an admin role. Adding and removing
project permissions should be very [straight forward](./projectpermissions.html).


