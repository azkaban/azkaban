API Documentation
========

| Often there's a desire to interact with Azkaban without having to use
  the web UI. Azkaban has some exposed ajax calls accessible through
  curl or some other HTTP request clients. All API calls require a
  proper authentication first.
| Azkaban assumes the following request header in servlet's
  ``isAjaxCall(HttpServletRequest request)`` method:

::

     Content-Type:     application/x-www-form-urlencoded
     X-Requested-With: XMLHttpRequest

However, currently for most of the APIs in this version, it is not checking
the request header. Many APIs still treat a request as an ajax call if
``request`` simply contains the parameter ``ajax``. Or even, several
APIs are implicitly assuming it is an ajax call even without this
keyword. For ease of use though, it is recommended to always keep the
correct request header.

.. _api-authenticate:

Authenticate
------------

-  **Method:** POST
-  **Request URL:** /?action=login
-  **Parameter Location:** Request Query String

This API helps authenticate a user and provides a ``session.id`` in
response.

Once a ``session.id`` has been returned, until the session expires, this
id can be used to do any API requests with a proper permission granted.
A session expires if you log out, change machines, browsers or
locations, if Azkaban is restarted, or if the session expires. The
default session timeout is 24 hours (one day). You can re-login whether
the session has expired or not. For the same user, a new session will
always override the old one.

**Importantly,** ``session.id`` should be provided for almost all API
calls (other than authentication). ``session.id`` can be simply appended
as one of the request parameters, or set via the cookie:
``azkaban.browser.session.id``. The two HTTP requests below are
equivalent:

.. code-block:: guess

   # a) Provide session.id parameter directly
   curl -k --get --data "session.id=bca1d75d-6bae-4163-a5b0-378a7d7b5a91&ajax=fetchflowgraph&project=azkaban-test-project&flow=test" https://localhost:8443/manager

   # b) Provide azkaban.browser.session.id cookie
   curl -k --get -b "azkaban.browser.session.id=bca1d75d-6bae-4163-a5b0-378a7d7b5a91" --data "ajax=fetchflowgraph&project=azkaban-test-project&flow=test" https://localhost:8443/manager

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+--------------+--------------------------------------------------+
| Parameter    | Description                                      |
+==============+==================================================+
| action=login | The fixed parameter indicating the login action. |
+--------------+--------------------------------------------------+
| username     | The Azkaban username.                            |
+--------------+--------------------------------------------------+
| password     | The corresponding password.                      |
+--------------+--------------------------------------------------+

**Response Object**
~~~~~~~~~~~~~~~~~~~

+------------+-----------------------------------------------------+
| Parameter  | Description                                         |
+============+=====================================================+
| error      | Return an error message if the login attempt fails. |
+------------+-----------------------------------------------------+
| session.id | Return a session id if the login attempt succeeds.  |
+------------+-----------------------------------------------------+

A sample call via curl:

.. code-block:: guess

   curl -k -X POST --data "action=login&username=azkaban&password=azkaban" https://localhost:8443

A sample response:

.. code-block:: guess

   {
     "status" : "success",
     "session.id" : "c001aba5-a90f-4daf-8f11-62330d034c0a"
   }

.. _api-create-a-project:

Create a Project
----------------

| The ajax API for creating a new project.
| **Notice:** before uploading any project zip files, the project should
  be created first via this API.

-  **Method:** POST
-  **Request URL:** /manager?action=create
-  **Parameter Location:** Request Query

.. _request-parameters-1:

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| session.id                        | The user session id.              |
+-----------------------------------+-----------------------------------+
| action=create                     | The fixed parameter indicating    |
|                                   | the create project action.        |
+-----------------------------------+-----------------------------------+
| name                              | The project name to be uploaded.  |
+-----------------------------------+-----------------------------------+
| description                       | The description for the project.  |
|                                   | This field cannot be empty.       |
+-----------------------------------+-----------------------------------+

**Response Object 1. (if the request succeeds):**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| status                            | The status of the creation        |
|                                   | attempt.                          |
+-----------------------------------+-----------------------------------+
| path                              | The url path to redirect          |
+-----------------------------------+-----------------------------------+
| action                            | The action that is suggested for  |
|                                   | the frontend to execute. (This is |
|                                   | designed for the usage of the     |
|                                   | Azkaban frontend javascripts,     |
|                                   | external users can ignore this    |
|                                   | field.)                           |
+-----------------------------------+-----------------------------------+

**Response Object 2. (if the request fails):**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+-----------+--------------------+
| Parameter | Description        |
+===========+====================+
| message   | The error message. |
+-----------+--------------------+
| error     | The error name.    |
+-----------+--------------------+

Here's a curl command sample:

.. code-block:: guess

   curl -k -X POST --data "session.id=9089beb2-576d-47e3-b040-86dbdc7f523e&name=aaaa&description=11" https://localhost:8443/manager?action=create

A sample response:

.. code-block:: guess

   {
     "status":"success",
     "path":"manager?project=aaaa",
     "action":"redirect"
   }

.. _api-delete-a-project:

Delete a Project
----------------

| The ajax API for deleting an existing project.
| **Notice:** Currently no response message will be returned after
  finishing the delete operation.

-  **Method:** GET
-  **Request URL:** /manager?delete=true
-  **Parameter Location:** Request Query

.. _request-parameters-2:

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| session.id                        | The user session id.              |
+-----------------------------------+-----------------------------------+
| delete=true                       | The fixed parameter to indicate   |
|                                   | the deleting project action.      |
+-----------------------------------+-----------------------------------+
| project                           | The project name to be deleted.   |
+-----------------------------------+-----------------------------------+

Here's a curl command sample:

.. code-block:: guess

   curl -k --get --data "session.id=bca1d75d-6bae-4163-a5b0-378a7d7b5a91&delete=true&project=test-delete-project" https://localhost:8443/manager

.. _api-upload-a-project-zip:

Upload a Project Zip
--------------------

| The ajax call to upload a project zip file. The zip file structure
  should follow the requirements described in `Upload
  Projects </docs/2.5/#upload-projects>`__.
| **Notice:** This API should be called after a project is successfully
  created.

-  **Method:** POST
-  **Content-Type:** multipart/mixed
-  **Request URL:** /manager?ajax=upload
-  **Parameter Location:** Request Body

.. _request-parameters-3:

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| session.id                        | The user session id.              |
+-----------------------------------+-----------------------------------+
| ajax=upload                       | The fixed parameter to the upload |
|                                   | action.                           |
+-----------------------------------+-----------------------------------+
| project                           | The project name to be uploaded.  |
+-----------------------------------+-----------------------------------+
| file                              | The project zip file. The type    |
|                                   | should be set as                  |
|                                   | ``application/zip`` or            |
|                                   | ``application/x-zip-compressed``. |
+-----------------------------------+-----------------------------------+

.. _response-object-1:

**Response Object**
~~~~~~~~~~~~~~~~~~~

+-----------+------------------------------------------------+
| Parameter | Description                                    |
+===========+================================================+
| error     | The error message if the upload attempt fails. |
+-----------+------------------------------------------------+
| projectId | The numerical id of the project                |
+-----------+------------------------------------------------+
| version   | The version number of the upload               |
+-----------+------------------------------------------------+

Here's a curl command sample:

.. code-block:: guess

   curl -k -i -X POST --form 'session.id=e7a29776-5783-49d7-afa0-b0e688096b5e' --form 'ajax=upload' --form 'file=@myproject.zip;type=application/zip' --form 'project=MyProject' https://localhost:8443/manager

A response sample:

.. code-block:: guess

   {
     "error" : "Installation Failed.\nError unzipping file.",
     "projectId" : "192",
     "version" : "1"
   }

.. _api-fetch-flows-of-a-project:

Fetch Flows of a Project
------------------------

Given a project name, this API call fetches all flow ids of that
project.

-  **Method:** GET
-  **Request URL:** /manager?ajax=fetchprojectflows
-  **Parameter Location:** Request Query String

.. _request-parameters-4:

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| session.id                        | The user session id.              |
+-----------------------------------+-----------------------------------+
| ajax=fetchprojectflows            | The fixed parameter indicating    |
|                                   | the fetchProjectFlows action.     |
+-----------------------------------+-----------------------------------+
| project                           | The project name to be fetched.   |
+-----------------------------------+-----------------------------------+

.. _response-object-2:

**Response Object**
~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| project                           | The project name.                 |
+-----------------------------------+-----------------------------------+
| projectId                         | The numerical id of the project.  |
+-----------------------------------+-----------------------------------+
| flows                             | A list of flow ids.               |
|                                   | **Example values:** [{"flowId":   |
|                                   | "aaa"}, {"flowId": "bbb"}]        |
+-----------------------------------+-----------------------------------+

Here's a curl command sample:

.. code-block:: guess

   curl -k --get --data "session.id=6c96e7d8-4df5-470d-88fe-259392c09eea&ajax=fetchprojectflows&project=azkaban-test-project" https://localhost:8443/manager

A response sample:

.. code-block:: guess

   {
     "project" : "test-azkaban",
     "projectId" : 192,
     "flows" : [ {
       "flowId" : "test"
     }, {
       "flowId" : "test2"
     } ]
   }

.. _api-fetch-jobs-of-a-flow:

Fetch Jobs of a Flow
--------------------

For a given project and a flow id, this API call fetches all the jobs
that belong to this flow. It also returns the corresponding graph
structure of those jobs.

-  **Method:** GET
-  **Request URL:** /manager?ajax=fetchflowgraph
-  **Parameter Location:** Request Query String

.. _request-parameters-5:

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| session.id                        | The user session id.              |
+-----------------------------------+-----------------------------------+
| ajax=fetchflowgraph               | The fixed parameter indicating    |
|                                   | the fetchProjectFlows action.     |
+-----------------------------------+-----------------------------------+
| project                           | The project name to be fetched.   |
+-----------------------------------+-----------------------------------+
| flow                              | The project id to be fetched.     |
+-----------------------------------+-----------------------------------+

.. _response-object-3:

**Response Object**
~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| project                           | The project name.                 |
+-----------------------------------+-----------------------------------+
| projectId                         | The numerical id of the project.  |
+-----------------------------------+-----------------------------------+
| flow                              | The flow id fetched.              |
+-----------------------------------+-----------------------------------+
| nodes                             | A list of job nodes belonging to  |
|                                   | this flow.                        |
|                                   | **Structure:**                    |
|                                   |                                   |
|                                   | ::                                |
|                                   |                                   |
|                                   |    {                              |
|                                   |      "id": "job.id"               |
|                                   |      "type": "job.type"           |
|                                   |      "in": ["job.ids that this job|
|                                   |      is directly depending upon.  |
|                                   |      Indirect ancestors are not in |
|                                   |      cluded in this list"]        |
|                                   |    }                              |
|                                   |                                   |
|                                   |                                   |
|                                   | **Example values:** [{"id":       |
|                                   | "first_job", "type": "java"},     |
|                                   | {"id": "second_job", "type":      |
|                                   | "command", "in":["first_job"]}]   |
+-----------------------------------+-----------------------------------+

Here's a curl command sample:

.. code-block:: guess

   curl -k --get --data "session.id=bca1d75d-6bae-4163-a5b0-378a7d7b5a91&ajax=fetchflowgraph&project=texter-1-1&flow=test" https://localhost:8445/manager

A response sample:

.. code-block:: guess

   {
     "project" : "azkaban-test-project",
     "nodes" : [ {
       "id" : "test-final",
       "type" : "command",
       "in" : [ "test-job-3" ]
     }, {
       "id" : "test-job-start",
       "type" : "java"
     }, {
       "id" : "test-job-3",
       "type" : "java",
       "in" : [ "test-job-2" ]
     }, {
       "id" : "test-job-2",
       "type" : "java",
       "in" : [ "test-job-start" ]
     } ],
     "flow" : "test",
     "projectId" : 192
   }

.. _api-fetch-executions-of-a-flow:

Fetch Executions of a Flow
--------------------------

Given a project name, and a certain flow, this API call provides a list
of corresponding executions. Those executions are sorted in descendent
submit time order. Also parameters are expected to specify the start
index and the length of the list. This is originally used to handle
pagination.

-  **Method:** GET
-  **Request URL:** /manager?ajax=fetchFlowExecutions
-  **Parameter Location:** Request Query String

.. _request-parameters-6:

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| session.id                        | The user session id.              |
+-----------------------------------+-----------------------------------+
| ajax=fetchFlowExecutions          | The fixed parameter indicating    |
|                                   | the fetchFlowExecutions action.   |
+-----------------------------------+-----------------------------------+
| project                           | The project name to be fetched.   |
+-----------------------------------+-----------------------------------+
| flow                              | The flow id to be fetched.        |
+-----------------------------------+-----------------------------------+
| start                             | The start index(inclusive) of the |
|                                   | returned list.                    |
+-----------------------------------+-----------------------------------+
| length                            | The max length of the returned    |
|                                   | list. For example, if the start   |
|                                   | index is 2, and the length is 10, |
|                                   | then the returned list will       |
|                                   | include executions of indices:    |
|                                   | [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]. |
+-----------------------------------+-----------------------------------+

.. _response-object-4:

**Response Object**
~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| executions                        | A list of execution objects, with |
|                                   | the requested start index and    |
|                                   | length.                           |
+-----------------------------------+-----------------------------------+
| total                             | The total number of all relevant  |
|                                   | execution                         |
+-----------------------------------+-----------------------------------+
| project                           | The project name fetched.         |
+-----------------------------------+-----------------------------------+
| projectId                         | The numerical project id fetched. |
+-----------------------------------+-----------------------------------+
| flow                              | The flow id fetched.              |
+-----------------------------------+-----------------------------------+
| from                              | The start index of the fetched    |
|                                   | executions                        |
+-----------------------------------+-----------------------------------+
| length                            | The length of the fetched         |
|                                   | executions.                       |
+-----------------------------------+-----------------------------------+

Here's a curl command sample:

.. code-block:: guess

   curl -k --get --data "session.id=6c96e7d8-4df5-470d-88fe-259392c09eea&ajax=fetchFlowExecutions&project=azkaban-test-project&flow=test&start=0&length=3" https://localhost:8443/manager

A response sample:

.. code-block:: guess

   {
     "executions" : [ {
       "startTime" : 1407779928865,
       "submitUser" : "1",
       "status" : "FAILED",
       "submitTime" : 1407779928829,
       "execId" : 306,
       "projectId" : 192,
       "endTime" : 1407779950602,
       "flowId" : "test"
     }, {
       "startTime" : 1407779877807,
       "submitUser" : "1",
       "status" : "FAILED",
       "submitTime" : 1407779877779,
       "execId" : 305,
       "projectId" : 192,
       "endTime" : 1407779899599,
       "flowId" : "test"
     }, {
       "startTime" : 1407779473354,
       "submitUser" : "1",
       "status" : "FAILED",
       "submitTime" : 1407779473318,
       "execId" : 304,
       "projectId" : 192,
       "endTime" : 1407779495093,
       "flowId" : "test"
     } ],
     "total" : 16,
     "project" : "azkaban-test-project",
     "length" : 3,
     "from" : 0,
     "flow" : "test",
     "projectId" : 192
   }

.. _api-fetch-running-executions-of-a-flow:

Fetch Running Executions of a Flow
----------------------------------

Given a project name and a flow id, this API call fetches only
executions that are currently running.

-  **Method:** GET
-  **Request URL:** /executor?ajax=getRunning
-  **Parameter Location:** Request Query String

.. _request-parameters-7:

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| session.id                        | The user session id.              |
+-----------------------------------+-----------------------------------+
| ajax=getRunning                   | The fixed parameter indicating    |
|                                   | the getRunning action.            |
+-----------------------------------+-----------------------------------+
| project                           | The project name to be fetched.   |
+-----------------------------------+-----------------------------------+
| flow                              | The flow id to be fetched.        |
+-----------------------------------+-----------------------------------+

.. _response-object-5:

**Response Object**
~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| execIds                           | A list of execution ids fetched.  |
|                                   | **Example values:** [301, 302,    |
|                                   | 111, 999]                         |
+-----------------------------------+-----------------------------------+

Here's a curl command sample:

.. code-block:: guess

   curl -k --data "session.id=34ba08fd-5cfa-4b65-94c4-9117aee48dda&ajax=getRunning&project=azkaban-test-project&flow=test" https://localhost:8443/executor

A response sample:

.. code-block:: guess

   {
     "execIds": [301, 302]
   }

.. _api-execute-a-flow:

Execute a Flow
--------------

This API executes a flow via an ajax call, supporting a rich selection
of different options. Running an individual job can also be achieved via
this API by disabling all other jobs in the same flow.

-  **Method:** GET
-  **Request URL:** /executor?ajax=executeFlow
-  **Parameter Location:** Request Query String

.. _request-parameters-8:

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| session.id                        | The user session id.              |
|                                   |                                   |
|                                   | **Example Values:**               |
|                                   | 30d538e2-4794-4e7e-8a35-25a9e2fd5 |
|                                   | 300                               |
+-----------------------------------+-----------------------------------+
| ajax=executeFlow                  | The fixed parameter indicating    |
|                                   | the current ajax action is        |
|                                   | executeFlow.                      |
+-----------------------------------+-----------------------------------+
| project                           | The project name of the executing |
|                                   | flow.                             |
|                                   |                                   |
|                                   | **Example Values:** run-all-jobs  |
+-----------------------------------+-----------------------------------+
| flow                              | The flow id to be executed.       |
|                                   |                                   |
|                                   | **Example Values:** test-flow     |
+-----------------------------------+-----------------------------------+
| disabled (optional)               | A list of job names that should   |
|                                   | be disabled for this execution.   |
|                                   | Should be formatted as a JSON     |
|                                   | Array String.                     |
|                                   |                                   |
|                                   | **Example Values:**               |
|                                   | ["job_name_1", "job_name_2",      |
|                                   | "job_name_N"]                     |
+-----------------------------------+-----------------------------------+
| successEmails (optional)          | A list of emails to be notified   |
|                                   | if the execution succeeds. All    |
|                                   | emails are delimited with        |
|                                   | [,|;|\\s+].                       |
|                                   |                                   |
|                                   | **Example Values:**               |
|                                   | foo@email.com,bar@email.com       |
+-----------------------------------+-----------------------------------+
| failureEmails (optional)          | A list of emails to be notified   |
|                                   | if the execution fails. All       |
|                                   | emails are delimited with        |
|                                   | [,|;|\\s+].                       |
|                                   |                                   |
|                                   | **Example Values:**               |
|                                   | foo@email.com,bar@email.com       |
+-----------------------------------+-----------------------------------+
| successEmailsOverride (optional)  | Whether uses system default email |
|                                   | settings to override              |
|                                   | successEmails.                    |
|                                   |                                   |
|                                   | **Possible Values:** true, false  |
+-----------------------------------+-----------------------------------+
| failureEmailsOverride (optional)  | Whether uses system default email |
|                                   | settings to override              |
|                                   | failureEmails.                    |
|                                   |                                   |
|                                   | **Possible Values:** true, false  |
+-----------------------------------+-----------------------------------+
| notifyFailureFirst (optional)     | Whether sends out email           |
|                                   | notifications as long as the      |
|                                   | first failure occurs.             |
|                                   |                                   |
|                                   | **Possible Values:** true, false  |
+-----------------------------------+-----------------------------------+
| notifyFailureLast (optional)      | Whether sends out email           |
|                                   | notifications as long as the last |
|                                   | failure occurs.                   |
|                                   |                                   |
|                                   | **Possible Values:** true, false  |
+-----------------------------------+-----------------------------------+
| failureAction (Optional)          | If a failure occurs, how should   |
|                                   | the execution behaves.            |
|                                   |                                   |
|                                   | **Possible Values:**              |
|                                   | finishCurrent, cancelImmediately, |
|                                   | finishPossible                    |
+-----------------------------------+-----------------------------------+
| concurrentOption (Optional)       | Concurrent choices. Use ignore if |
|                                   | nothing specific is required.   |
|                                   |                                   |
|                                   | **Possible Values:** ignore,      |
|                                   | pipeline, skip                    |
+-----------------------------------+-----------------------------------+
| flowOverride[flowProperty]        | Override specified flow property  |
| (Optional)                        | with specified value.             |
|                                   |                                   |
|                                   | **Example Values :**              |
|                                   | flowOverride[failure.email]=test@ |
|                                   | gmail.com                         |
+-----------------------------------+-----------------------------------+

.. _response-object-6:

**Response Object**
~~~~~~~~~~~~~~~~~~~

+-----------+--------------------------------------+
| Parameter | Description                          |
+===========+======================================+
| error     | Error message if the call has failed |
+-----------+--------------------------------------+
| flow      | The executed flow id                 |
+-----------+--------------------------------------+
| execid    | The execution id                     |
+-----------+--------------------------------------+

Here is a curl command example:

.. code-block:: guess

   curl -k --get --data 'session.id=189b956b-f39f-421e-9a95-e3117e7543c9' --data 'ajax=executeFlow' --data 'project=azkaban-test-project' --data 'flow=test' https://localhost:8443/executor

Sample response:

.. code-block:: guess

   {
     message: "Execution submitted successfully with exec id 295",
     project: "foo-demo",
     flow: "test",
     execid: 295
   }

.. _api-cancel-a-flow-execution:

Cancel a Flow Execution
-----------------------

Given an execution id, this API call cancels a running flow. If the flow
is not running, it will return an error message.

-  **Method:** GET
-  **Request URL:** /executor?ajax=cancelFlow
-  **Parameter Location:** Request Query String

.. _request-parameters-9:

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| session.id                        | The user session id.              |
+-----------------------------------+-----------------------------------+
| ajax=cancelFlow                   | The fixed parameter indicating    |
|                                   | the current ajax action is        |
|                                   | cancelFlow.                       |
+-----------------------------------+-----------------------------------+
| execid                            | The execution id.                 |
+-----------------------------------+-----------------------------------+

Here's a curl command sample:

.. code-block:: guess

   curl -k --data "session.id=34ba08fd-5cfa-4b65-94c4-9117aee48dda&ajax=cancelFlow&execid=302" https://localhost:8443/executor

A response sample if succeeds:

.. code-block:: guess

   { }

A response sample if fails:

.. code-block:: guess

   {
     "error" : "Execution 302 of flow test isn't running."
   }

.. _api-schedule-a-flow:

Schedule a period-based Flow (Deprecated)
-----------------------------------------

This API call schedules a period-based flow.

-  **Method:** POST
-  **Request URL:** /schedule?ajax=scheduleFlow
-  **Parameter Location:** Request Query String

.. _request-parameters-10:

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| session.id                        | The user session id.              |
+-----------------------------------+-----------------------------------+
| ajax=scheduleFlow                 | The fixed parameter indicating    |
|                                   | the action is to schedule a flow. |
+-----------------------------------+-----------------------------------+
| projectName                       | The name of the project.          |
+-----------------------------------+-----------------------------------+
| projectId                         | The id of the project. You can    |
|                                   | find this with `Fetch Flows of a  |
|                                   | Project <#api-fetch-flows-of-a-pr |
|                                   | oject>`__.                        |
+-----------------------------------+-----------------------------------+
| flowName                          | The name of the flow.             |
+-----------------------------------+-----------------------------------+
| scheduleTime(with timezone)       | The time to schedule the flow.    |
|                                   | Example: 12,00,pm,PDT (Unless UTC |
|                                   | is specified, Azkaban will take   |
|                                   | current server's default timezone |
|                                   | instead)                          |
+-----------------------------------+-----------------------------------+
| scheduleDate                      | The date to schedule the flow.    |
|                                   | Example: 07/22/2014               |
+-----------------------------------+-----------------------------------+
| is_recurring=on (optional)        | Flags the schedule as a recurring |
|                                   | schedule.                         |
+-----------------------------------+-----------------------------------+
| period (optional)                 | Specifies the recursion period.   |
|                                   | Depends on the "is_recurring"     |
|                                   | flag being set. Example: 5w       |
|                                   | **Possible Values:**              |
|                                   |                                   |
|                                   | +---+---------+                   |
|                                   | | M | Months  |                   |
|                                   | +---+---------+                   |
|                                   | | w | Weeks   |                   |
|                                   | +---+---------+                   |
|                                   | | d | Days    |                   |
|                                   | +---+---------+                   |
|                                   | | h | Hours   |                   |
|                                   | +---+---------+                   |
|                                   | | m | Minutes |                   |
|                                   | +---+---------+                   |
|                                   | | s | Seconds |                   |
|                                   | +---+---------+                   |
+-----------------------------------+-----------------------------------+

Here's a curl command sample:

.. code-block:: guess


     # a) One time schedule
     curl -k https://HOST:PORT/schedule -d "ajax=scheduleFlow&projectName=PROJECT_NAME&flow=FLOW_NAME&projectId=PROJECT_ID&scheduleTime=12,00,pm,PDT&scheduleDate=07/22/2014" -b azkaban.browser.session.id=SESSION_ID

     # b) Recurring schedule
     curl -k https://HOST:PORT/schedule -d "ajax=scheduleFlow&is_recurring=on&period=5w&projectName=PROJECT_NAME&flow=FLOW_NAME&projectId=PROJECT_ID&scheduleTime=12,00,pm,PDT&scheduleDate=07/22/2014" -b azkaban.browser.session.id=SESSION_ID

An example success response:

.. code-block:: guess

   {
     "message" : "PROJECT_NAME.FLOW_NAME scheduled.",
     "status" : "success"
   }

An example failure response:

.. code-block:: guess

   {
     "message" : "Permission denied. Cannot execute FLOW_NAME",
     "status" : "error"
   }

An example failure response for invalid schedule period:

.. code-block:: guess

   {
     "message" : "PROJECT_NAME.FLOW_NAME scheduled.",
     "error" : "Invalid schedule period unit 'A",
     "status" : "success"
   }

.. _api-flexible-schedule:

Flexible scheduling using Cron
------------------------------

This API call schedules a flow by a cron Expression. Cron is a UNIX tool
that has been widely used for a long time, and we use `Quartz
library <http://www.quartz-scheduler.org/>`__ to parse cron Expression.
All cron schedules follow the timezone defined in azkaban web server
(the timezone ID is obtained by
*java.util.TimeZone.getDefault().getID()*).

-  **Method:** POST
-  **Request URL:** /schedule?ajax=scheduleCronFlow
-  **Parameter Location:** Request Query String

.. _request-parameters-11:

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| session.id                        | The user session id.              |
+-----------------------------------+-----------------------------------+
| ajax=scheduleCronFlow             | The fixed parameter indicating    |
|                                   | the action is to use cron to      |
|                                   | schedule a flow.                  |
+-----------------------------------+-----------------------------------+
| projectName                       | The name of the project.          |
+-----------------------------------+-----------------------------------+
| flow                              | The name of the flow.             |
+-----------------------------------+-----------------------------------+
| cronExpression                    | A CRON expression is a string     |
|                                   | comprising 6 or 7 fields          |
|                                   | separated by white space that     |
|                                   | represents a set of times. In     |
|                                   | azkaban, we use `Quartz Cron      |
|                                   | Format <http://www.quartz-schedul |
|                                   | er.org/documentation/quartz-2.x/t |
|                                   | utorials/crontrigger.html>`__.    |
+-----------------------------------+-----------------------------------+

Here's a curl command sample:

.. code-block:: guess

   curl -k -d ajax=scheduleCronFlow -d projectName=wtwt -d flow=azkaban-training --data-urlencode cronExpression="0 23/30 5,7-10 ? * 6#3" -b "azkaban.browser.session.id=XXXXXXXXXXXXXX" http://localhost:8081/schedule

An example success response:

.. code-block:: guess

   {
     "message" : "PROJECT_NAME.FLOW_NAME scheduled.",
     "scheduleId" : SCHEDULE_ID,
     "status" : "success"
   }

An example failure response:

.. code-block:: guess

   {
     "message" : "Cron expression must exist.",
     "status" : "error"
   }

.. code-block:: guess

   {
     "message" : "Permission denied. Cannot execute FLOW_NAME",
     "status" : "error"
   }

An example failure response for invalid cron expression:

.. code-block:: guess

   {
     "message" : "This expression <*****> can not be parsed to quartz cron.",
     "status" : "error"
   }

.. _api-fetch-schedule:

Fetch a Schedule
----------------

Given a project id and a flow id, this API call fetches the schedule.

-  **Method:** GET
-  **Request URL:** /schedule?ajax=fetchSchedule
-  **Parameter Location:** Request Query String

.. _request-parameters-12:

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+--------------------+----------------------------------------------+
| Parameter          | Description                                  |
+====================+==============================================+
| session.id         | The user session id.                         |
+--------------------+----------------------------------------------+
| ajax=fetchSchedule | The fixed parameter indicating the schedule. |
+--------------------+----------------------------------------------+
| projectId          | The id of the project.                       |
+--------------------+----------------------------------------------+
| flowId             | The name of the flow.                        |
+--------------------+----------------------------------------------+

Here's a curl command sample:

.. code-block:: guess

   curl -k --get --data "session.id=XXXXXXXXXXXXXX&ajax=fetchSchedule&projectId=1&flowId=test" http://localhost:8081/schedule

An example success response:

.. code-block:: guess

   {
     "schedule" : {
       "cronExpression" : "0 * 9 ? * *",
       "nextExecTime" : "2017-04-01 09:00:00",
       "period" : "null",
       "submitUser" : "azkaban",
       "executionOptions" : {
         "notifyOnFirstFailure" : false,
         "notifyOnLastFailure" : false,
         "failureEmails" : [ ],
         "successEmails" : [ ],
         "pipelineLevel" : null,
         "queueLevel" : 0,
         "concurrentOption" : "skip",
         "mailCreator" : "default",
         "memoryCheck" : true,
         "flowParameters" : {
         },
         "failureAction" : "FINISH_CURRENTLY_RUNNING",
         "failureEmailsOverridden" : false,
         "successEmailsOverridden" : false,
         "pipelineExecutionId" : null,
         "disabledJobs" : [ ]
       },
       "scheduleId" : "3",
       "firstSchedTime" : "2017-03-31 11:45:21"
     }
   }

If there is no schedule, empty response returns.

.. code-block:: guess

   {}

.. _api-unschedule-a-flow:

Unschedule a Flow
-----------------

This API call unschedules a flow.

-  **Method:** POST
-  **Request URL:** /schedule?action=removeSched
-  **Parameter Location:** Request Query String

.. _request-parameters-13:

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| session.id                        | The user session id.              |
+-----------------------------------+-----------------------------------+
| action=removeSched                | The fixed parameter indicating    |
|                                   | the action is to unschedule a     |
|                                   | flow.                             |
+-----------------------------------+-----------------------------------+
| scheduleId                        | The id of the schedule. You can   |
|                                   | find this in the Azkaban UI on    |
|                                   | the /schedule page.               |
+-----------------------------------+-----------------------------------+

Here's a curl command sample:

.. code-block:: guess

   curl -k https://HOST:PORT/schedule -d "action=removeSched&scheduleId=SCHEDULE_ID" -b azkaban.browser.session.id=SESSION_ID

An example success response:

.. code-block:: guess

   {
     "message" : "flow FLOW_NAME removed from Schedules.",
     "status" : "success"
   }

An example failure response:

.. code-block:: guess

   {
     "message" : "Schedule with ID SCHEDULE_ID does not exist",
     "status" : "error"
   }

.. _api-set-sla:

Set a SLA
---------

This API call sets a SLA.

-  **Method:** POST
-  **Request URL:** /schedule?ajax=setSla
-  **Parameter Location:** Request Query String

.. _request-parameters-14:

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| session.id                        | The user session id.              |
+-----------------------------------+-----------------------------------+
| ajax=setSla                       | The fixed parameter indicating    |
|                                   | the action is to set a SLA.       |
+-----------------------------------+-----------------------------------+
| scheduleId                        | The id of the schedule. You can  |
|                                   | find this with `Fetch a           |
|                                   | Schedule <#api-fetch-schedule>`__ |
|                                   | .                                 |
+-----------------------------------+-----------------------------------+
| slaEmails                         | A list of SLA alert emails.       |
|                                   | **Example:**                      |
|                                   | slaEmails=a@example.com;b@example |
|                                   | .com                              |
+-----------------------------------+-----------------------------------+
| settings[...]                     | Rules of SLA. Format is           |
|                                   | settings[...]=[id],[rule],[durati |
|                                   | on],[emailAction],[killAction].   |
|                                   | **Example:**                      |
|                                   | settings[0]=aaa,SUCCESS,5:00,true |
|                                   | ,false                            |
+-----------------------------------+-----------------------------------+

Here's a curl command sample:

.. code-block:: guess

   curl -k -d "ajax=setSla&scheduleId=1&slaEmails=a@example.com;b@example.com&settings[0]=aaa,SUCCESS,5:00,true,false&settings[1]=bbb,SUCCESS,10:00,false,true" -b "azkaban.browser.session.id=XXXXXXXXXXXXXX" "http://localhost:8081/schedule"

An example success response:

.. code-block:: guess

   {}

An example failure response:

.. code-block:: guess

   {
     "error" : "azkaban.scheduler.ScheduleManagerException: Unable to parse duration for a SLA that needs to take actions!"
   }

.. _api-fetch-sla:

Fetch a SLA
-----------

Given a schedule id, this API call fetches the SLA.

-  **Method:** GET
-  **Request URL:** /schedule?ajax=slaInfo
-  **Parameter Location:** Request Query String

.. _request-parameters-15:

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| session.id                        | The user session id.              |
+-----------------------------------+-----------------------------------+
| ajax=slaInfo                      | The fixed parameter indicating    |
|                                   | the SLA.                          |
+-----------------------------------+-----------------------------------+
| scheduleId                        | The id of the schedule. You can  |
|                                   | find this with `Fetch a           |
|                                   | Schedule <#api-fetch-schedule>`__ |
|                                   | .                                 |
+-----------------------------------+-----------------------------------+

Here's a curl command sample:

.. code-block:: guess

   curl -k --get --data "session.id=XXXXXXXXXXXXXX&ajax=slaInfo&scheduleId=1" http://localhost:8081/schedule"

An example success response:

.. code-block:: guess

   {
     "settings" : [ {
       "duration" : "300m",
       "rule" : "SUCCESS",
       "id" : "aaa",
       "actions" : [ "EMAIL" ]
     }, {
       "duration" : "600m",
       "rule" : "SUCCESS",
       "id" : "bbb",
       "actions" : [ "KILL" ]
     } ],
     "slaEmails" : [ "a@example.com", "b@example.com" ],
     "allJobNames" : [ "aaa", "ccc", "bbb", "start", "end" ]
   }

.. _api-pause-a-flow-execution:

Pause a Flow Execution
----------------------

Given an execution id, this API pauses a running flow. If an execution
has already been paused, it will not return any error; if an execution
is not running, it will return an error message.

-  **Method:** GET
-  **Request URL:** /executor?ajax=pauseFlow
-  **Parameter Location:** Request Query String

.. _request-parameters-16:

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| session.id                        | The user session id.              |
+-----------------------------------+-----------------------------------+
| ajax=pauseFlow                    | The fixed parameter indicating    |
|                                   | the current ajax action is        |
|                                   | pauseFlow.                        |
+-----------------------------------+-----------------------------------+
| execid                            | The execution id.                 |
+-----------------------------------+-----------------------------------+

Here's a curl command sample:

.. code-block:: guess

   curl -k --data "session.id=34ba08fd-5cfa-4b65-94c4-9117aee48dda&ajax=pauseFlow&execid=303" https://localhost:8443/executor

A response sample (if succeeds, or pauseFlow is called multiple times):

.. code-block:: guess

   { }

A response sample (if fails, only when the flow is not actually
running):

.. code-block:: guess

   {
     "error" : "Execution 303 of flow test isn't running."
   }

.. _api-resume-a-flow-execution:

Resume a Flow Execution
-----------------------

Given an execution id, this API resumes a paused running flow. If an
execution has already been resumed, it will not return any errors; if an
execution is not running, it will return an error message.

-  **Method:** GET
-  **Request URL:** /executor?ajax=resumeFlow
-  **Parameter Location:** Request Query String

.. _request-parameters-17:

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| session.id                        | The user session id.              |
+-----------------------------------+-----------------------------------+
| ajax=resumeFlow                   | The fixed parameter indicating    |
|                                   | the current ajax action is        |
|                                   | resumeFlow.                       |
+-----------------------------------+-----------------------------------+
| execid                            | The execution id.                 |
+-----------------------------------+-----------------------------------+

Here's a curl command sample:

.. code-block:: guess

   curl -k --data "session.id=34ba08fd-5cfa-4b65-94c4-9117aee48dda&ajax=resumeFlow&execid=303" https://localhost:8443/executor

A response sample (if succeeds, or resumeFlow is called multiple times):

.. code-block:: guess

   { }

A response sample (if fails, only when the flow is not actually
running):

.. code-block:: guess

   {
     "error" : "Execution 303 of flow test isn't running."
   }

.. _api-fetch-a-flow-execution:

Fetch a Flow Execution
----------------------

Given an execution id, this API call fetches all the detailed
information of that execution, including a list of all the job
executions.

-  **Method:** GET
-  **Request URL:** /executor?ajax=fetchexecflow
-  **Parameter Location:** Request Query String

.. _request-parameters-18:

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| session.id                        | The user session id.              |
+-----------------------------------+-----------------------------------+
| ajax=fetchexecflow                | The fixed parameter indicating    |
|                                   | the fetchexecflow action.         |
+-----------------------------------+-----------------------------------+
| execid                            | The execution id to be fetched.   |
+-----------------------------------+-----------------------------------+

.. _response-object-7:

**Response Object**
~~~~~~~~~~~~~~~~~~~

It returns detailed information about the execution (check the example
below). One thing to notice is that the field ``nodes[i].in`` actually
indicates what are the dependencies of this node.

Here's a curl command sample:

.. code-block:: guess

   curl -k --data "session.id=34ba08fd-5cfa-4b65-94c4-9117aee48dda&ajax=fetchexecflow&execid=304" https://localhost:8443/executor

A response sample:

.. code-block:: guess

   {
     "attempt" : 0,
     "submitUser" : "1",
     "updateTime" : 1407779495095,
     "status" : "FAILED",
     "submitTime" : 1407779473318,
     "projectId" : 192,
     "flow" : "test",
     "endTime" : 1407779495093,
     "type" : null,
     "nestedId" : "test",
     "startTime" : 1407779473354,
     "id" : "test",
     "project" : "test-azkaban",
     "nodes" : [ {
       "attempt" : 0,
       "startTime" : 1407779495077,
       "id" : "test",
       "updateTime" : 1407779495077,
       "status" : "CANCELLED",
       "nestedId" : "test",
       "type" : "command",
       "endTime" : 1407779495077,
       "in" : [ "test-foo" ]
     }, {
       "attempt" : 0,
       "startTime" : 1407779473357,
       "id" : "test-bar",
       "updateTime" : 1407779484241,
       "status" : "SUCCEEDED",
       "nestedId" : "test-bar",
       "type" : "pig",
       "endTime" : 1407779484236
     }, {
       "attempt" : 0,
       "startTime" : 1407779484240,
       "id" : "test-foobar",
       "updateTime" : 1407779495073,
       "status" : "FAILED",
       "nestedId" : "test-foobar",
       "type" : "java",
       "endTime" : 1407779495068,
       "in" : [ "test-bar" ]
     }, {
       "attempt" : 0,
       "startTime" : 1407779495069,
       "id" : "test-foo",
       "updateTime" : 1407779495069,
       "status" : "CANCELLED",
       "nestedId" : "test-foo",
       "type" : "java",
       "endTime" : 1407779495069,
       "in" : [ "test-foobar" ]
     } ],
     "flowId" : "test",
     "execid" : 304
   }

.. _api-fetch-execution-job-logs:

Fetch Execution Job Logs
------------------------

Given an execution id and a job id, this API call fetches the
corresponding job logs. The log text can be quite large sometimes, so
this API call also expects the parameters ``offset`` and ``length`` to
be specified.

-  **Method:** GET
-  **Request URL:** /executor?ajax=fetchExecJobLogs
-  **Parameter Location:** Request Query String

.. _request-parameters-19:

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| session.id                        | The user session id.              |
+-----------------------------------+-----------------------------------+
| ajax=fetchExecJobLogs             | The fixed parameter indicating    |
|                                   | the fetchExecJobLogs action.      |
+-----------------------------------+-----------------------------------+
| execid                            | The unique id for an execution.   |
+-----------------------------------+-----------------------------------+
| jobId                             | The unique id for the job to be   |
|                                   | fetched.                          |
+-----------------------------------+-----------------------------------+
| offset                            | The offset for the log data.      |
+-----------------------------------+-----------------------------------+
| length                            | The length of the log data. For   |
|                                   | example, if the offset set is 10  |
|                                   | and the length is 1000, the       |
|                                   | returned log will starts from the |
|                                   | 10th character and has a length   |
|                                   | of 1000 (less if the remaining    |
|                                   | log is less than 1000 long).      |
+-----------------------------------+-----------------------------------+

.. _response-object-8:

**Response Object**
~~~~~~~~~~~~~~~~~~~

+-----------+------------------------------+
| Parameter | Description                  |
+===========+==============================+
| data      | The text data of the logs.   |
+-----------+------------------------------+
| offset    | The offset for the log data. |
+-----------+------------------------------+
| length    | The length of the log data.  |
+-----------+------------------------------+

Here's a curl command sample:

.. code-block:: guess

   curl -k --data "session.id=9089beb2-576d-47e3-b040-86dbdc7f523e&ajax=fetchExecJobLogs&execid=297&jobId=test-foobar&offset=0&length=100" https://localhost:8443/executor

A response sample:

.. code-block:: guess

   {
     "data" : "05-08-2014 16:53:02 PDT test-foobar INFO - Starting job test-foobar at 140728278",
     "length" : 100,
     "offset" : 0
   }

.. _api-fetch-flow-execution-updates:

Fetch Flow Execution Updates
----------------------------

This API call fetches the updated information for an execution. It
filters by ``lastUpdateTime`` which only returns job information updated
afterwards.

-  **Method:** GET
-  **Request URL:** /executor?ajax=fetchexecflowupdate
-  **Parameter Location:** Request Query String

.. _request-parameters-20:

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| session.id                        | The user session id.              |
+-----------------------------------+-----------------------------------+
| ajax=fetchexecflowupdate          | The fixed parameter indicating    |
|                                   | the fetch execution updates       |
|                                   | action.                           |
+-----------------------------------+-----------------------------------+
| execid                            | The execution id.                 |
+-----------------------------------+-----------------------------------+
| lastUpdateTime                    | The criteria to filter by last    |
|                                   | update time. Set the value to be  |
|                                   | ``-1`` if all job information are |
|                                   | needed.                           |
+-----------------------------------+-----------------------------------+

.. _response-object-9:

**Response Object**
~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| id                                | The flow id.                      |
+-----------------------------------+-----------------------------------+
| flow                              | The flow name.                    |
+-----------------------------------+-----------------------------------+
| startTime                         | The start time of this flow       |
|                                   | execution.                        |
+-----------------------------------+-----------------------------------+
| updateTime                        | The last updated time of this     |
|                                   | flow execution.                   |
+-----------------------------------+-----------------------------------+
| endTime                           | The end time of this flow         |
|                                   | execution (if it finishes).       |
+-----------------------------------+-----------------------------------+
| status                            | The current status of the flow.   |
+-----------------------------------+-----------------------------------+
| attempt                           | The attempt number of this flow   |
|                                   | execution.                        |
+-----------------------------------+-----------------------------------+
| nodes                             | Information for each execution    |
|                                   | job. Containing the following     |
|                                   | fields:                           |
|                                   | ::                                |
|                                   |                                   |
|                                   |    {                              |
|                                   |      "attempt": String,           |
|                                   |      "startTime": Number,         |
|                                   |      "id": String (the job id),   |
|                                   |      "updateTime":Number,         |
|                                   |      "status": String,            |
|                                   |      "endTime": Number            |
|                                   |    }                              |
|                                   |                                   |
+-----------------------------------+-----------------------------------+

Here's a curl command sample:

.. code-block:: guess

   curl -k --data "execid=301&lastUpdateTime=-1&session.id=6668c180-efe7-46a-8dd2-e36508b440d8" https://localhost:8443/executor?ajax=fetchexecflowupdate

A response sample:

.. code-block:: guess

   {
     "id" : "test",
     "startTime" : 1407778382894,
     "attempt" : 0,
     "status" : "FAILED",
     "updateTime" : 1407778404708,
     "nodes" : [ {
       "attempt" : 0,
       "startTime" : 1407778404683,
       "id" : "test",
       "updateTime" : 1407778404683,
       "status" : "CANCELLED",
       "endTime" : 1407778404683
     }, {
       "attempt" : 0,
       "startTime" : 1407778382913,
       "id" : "test-job-1",
       "updateTime" : 1407778393850,
       "status" : "SUCCEEDED",
       "endTime" : 1407778393845
     }, {
       "attempt" : 0,
       "startTime" : 1407778393849,
       "id" : "test-job-2",
       "updateTime" : 1407778404679,
       "status" : "FAILED",
       "endTime" : 1407778404675
     }, {
       "attempt" : 0,
       "startTime" : 1407778404675,
       "id" : "test-job-3",
       "updateTime" : 1407778404675,
       "status" : "CANCELLED",
       "endTime" : 1407778404675
     } ],
     "flow" : "test",
     "endTime" : 1407778404705
   }

Fetch Logs of a Project
------------------------

Given a project name, this API call fetches all logs of a project.

-  **Method:** GET
-  **Request URL:** /manager?ajax=fetchProjectLogs
-  **Parameter Location:** Request Query String

.. _request-parameters-4:

**Request Parameters**
~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| session.id                        | The user session id.              |
+-----------------------------------+-----------------------------------+
| ajax=fetchProjectLogs             | The fixed parameter indicating    |
|                                   | the fetchProjectLogs action.      |
+-----------------------------------+-----------------------------------+
| project                           | The project name to be fetched.   |
+-----------------------------------+-----------------------------------+

.. _response-object-2:

**Response Object**
~~~~~~~~~~~~~~~~~~~

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| project                           | The project name.                 |
+-----------------------------------+-----------------------------------+
| projectId                         | The numerical id of the project.  |
+-----------------------------------+-----------------------------------+
| columns                           | "user", "time", "type", "message" |
|                                   | columns                           |
+-----------------------------------+-----------------------------------+
| logData                           | Array of log data                 |
|                                   | **Example values:** [             |
|                                   | [ "test_user",                    |
|                                   |   1540885820913,                  |
|                                   |   "PROPERTY_OVERRIDE",            | 
|                                   |   "some description" ],           | 
|                                   | [ ... ], [ ... ],  ]              |
+-----------------------------------+-----------------------------------+

Here's a curl command sample:

.. code-block:: guess

   curl -k --get --data "session.id=6c96e7d8-4df5-470d-88fe-259392c09eea&ajax=fetchProjectLogs&project=azkaban-test-project" https://localhost:8443/manager

A response sample:

.. code-block:: guess

{
  "columns" : [ "user", "time", "type", "message" ],
  "logData" : [ 
    [ "test_user1", 1543615522936, "PROPERTY_OVERRIDE", "Modified Properties: .... " ],
    [ "test_user2", 1542346639933, "UPLOADED", "Uploaded project files zip " ],
    [ "test_user3", 1519908889338, "CREATED", null ],
    ... 
  ],
  "project" : "azkaban-test-project",
  "projectId" : 1
}
