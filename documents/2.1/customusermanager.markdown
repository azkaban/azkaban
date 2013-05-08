---
layout: documents
nav: customusermanager
context: ../..
---

#Custom User Manager

Although the XmlUserManager is easy enough to get started with, you may want
to integrate with an already established directory system, such as LDAP.

It should be fairly straight forward to implement a custom UserManager.

The UserManager is a java interface. There are only a few methods needed to implement.

<br/>
<pre class="code">
public interface UserManager {
	public User getUser(String username, String password) throws UserManagerException;
	public boolean validateUser(String username);
	public boolean validateGroup(String group);
	public Role getRole(String roleName);
	public boolean validateProxyUser(String proxyUser, User realUser);
}
</pre>
<br/>

The constructor should take an _azkaban.utils.Props_ object. The contents of _azkaban.properties_ will
be available for the UserManager for configuration.

<br/>

Package your new custom UserManager into a jar and drop it into the ./extlib directory or 
alternatively into the plugins directory at _./plugins/_name/_of/_auth_/_custom jar_ (i.e. _./plugins/ldap/linkedin-ldap.jar_).

<br/>

Change the _azkaban.properties_ configuration to point to the custom UserManager. Add additional
parameters into _azkaban.properties_ if needed by your custom user manager.

<br/>

|{.parameter}Parameter |{.default}Default               |
|----------------------|--------------------------------|
|user.manager.class    | azkaban.user.CustomUserManager |
{.params}





