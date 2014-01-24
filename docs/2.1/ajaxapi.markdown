---
layout: documents
nav: ajaxapi
context: ../..
---

#Ajax API

Often there's a desire to interact with Azkaban without having to use the web ui.
Azkaban has some exposed ajax calls you can make to the web server through curl or some other Http get or post methods.
However, you must have a secure session to call these services. The following are some of the useful calls you can make.
All ajax responses will be in json format.

<hr/>

### Authenticated Session
To get a secure session, make a POST request to Azkaban's base url. The parameters should be:
<pre class="code">
action=login
username=&lt;username&gt;
password=&lt;password&gt;
</pre>

<br/>
The response will be json with the following parameters set:

|{.parameter}Parameter			|{.description} Description             |
|-------------------------------|---------------------------------------|
|error 	| Error message if authentication has failed |
|session.id | If login has occurred successfully, this is the session id that will be returned |
{.params}

This session id must be submitted with all subsequent requests into Azkaban.

Once a session id has been returned, until the session has expired, this id can be used to do further requests into Azkaban.
A session can expire if you log out, change machines or location, Azkaban is restarted or if the session timed out. By default
session timeout is a day. You can re-login if the session expires.

<pre class="code">
curl -k --data "action=login&amp;username=azkaban&amp;password=azkaban" https://localhost:8443
</pre>

<hr/>

### Upload Project Zip
Uploads are multipart POST messages to __/manager__. The following parameters must be set do to the file upload. The content type
should be _application/zip_ or _application/x-zip-compressed_

<pre class="code">
session.id=&lt;session.id&gt;
ajax=upload
project=&lt;projectName&gt;
file=$lt;file&gt;
</pre> 

The response will be json with the following parameters set:

|{.parameter}Parameter			|{.description} Description             |
|-------------------------------|---------------------------------------|
|error 	| Error message if the call has failed|
|projectId | The numerical id of the project |
|version | The version # of the upload |
{.params}

Here's sample a curl command.
<pre class="code">
curl -k -i -H "Content-Type: multipart/mixed" -X POST --form 'session.id=e7a29776-5783-49d7-afa0-b0e688096b5e' --form 'ajax=upload' --form 'file=@myproject.zip;type=application/zip' --form 'project=MyProject;type/plain' https://localhost:8443/manager
</pre>

<hr/>

### Executing Flow

<pre class="code">
session.id=&lt;session.id&gt;
ajax=executeFlow
project=&lt;projectName&gt;
flow=&lt;flowName&gt;
</pre>

|{.parameter}Parameter			|{.description} Description             |
|-------------------------------|---------------------------------------|
|error 	| Error message if the call has failed |
|flow | The executed flow id |
|execid | The execution id |
{.params}



