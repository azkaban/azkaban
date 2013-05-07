---
layout: documents
nav: executions
context: ../..
---

#Executions

##Flow Execution Page
After [executing a flow](./executingflow.html) you will be presented the Executing Flow page.  Alternatively, you can
access these flows from the [Flow View](./projectflowview.html) page under the Executions tab, the History page, or the Executing page.

This page is similar to the Flow View page, except it shows status of running jobs. 

<img class="shadowimg" title="Executing Flow" src="./images/executingflowpage.png" ALT="Executing Flow Page" width="450" />

Selecting the Job List will give a timeline of job executions. You can access the jobs and job logs directly from this list.

<img class="shadowimg" title="Executing Flow" src="./images/executingflowpagejobslist.png" ALT="Executing Flow Page" width="450" />

This page will auto update as long as the execution is not finished.

Some options that you are able to do on execution flows include the following:
* __Cancel__ -kills all running jobs and fails the flow immediately. The flow state will be KILLED.
* __Pause__ -prevents new jobs from running. Currently running jobs proceed as usual.
* __Resume__ -resume a paused execution.
* __Retry Failed__ -only available when the flow is in a FAILED FINISHING state. Retry will restart all FAILED jobs while the flow is still active. Attempts will appear in the Jobs List page.
* __Prepare Execution__ -only available on a finished flow, regardless of success or failures. This will auto disable successfully completed jobs.

##Executing Page

Clicking on the Executing Tab in the header will show the Execution page. This page will show currently running executions as well as recently finished flows.

<img class="shadowimg" title="Executions" src="./images/executingflowspage.png" ALT="Executions" width="450" />

##History Page
Currently executing flows as well as completed executions will appear in the History page. Searching options are provided to find the execution you're looking for.
Alternatively, you can view previous executions for a flow on the [Flow View](./projectflowview.html) execution tab.

<img class="shadowimg" title="Executions" src="./images/historypage.png" ALT="Executions" width="450" />