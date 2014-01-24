---
layout: documents
nav: jobpage
context: ../..
---

#Jobs
Jobs make up individual tasks of a flow. To get to the jobs page, you can right click on a job in the Flow View,
the Executing Flow view or the Project Page.

<img class="shadowimg" title="Jobs Page" src="./images/jobpage.png" ALT="Jobs Page" width="450" />

From this page you can see the dependencies and dependents for a job as well as the global properties that the
job will use.

### Job Edit
Clicking on Job Edit will allow you to edit all the job properties except for certain reserved parameters, such as _type_,
and _dependencies_. The changes to the parameters will affect an executing flow only if the job hasn't started to run yet.
These overwrites of job properties will be overwritten by the next project upload.

<img class="shadowimg" title="Jobs Page" src="./images/jobedit.png" ALT="Jobs Edit" width="450" />

### Job History
The Job history page will show a history of executions for a job in a flow as well as access to their logs. The logs
have a retention policy and is by default purged after a month, although the execution information persists. The graph shows
the runtimes for the jobs that appear on that page.

Any retries of a job will show as _executionid.attempt number_.

<img class="shadowimg" title="Job History" src="./images/jobhistorypage.png" ALT="Job History" width="450" />
