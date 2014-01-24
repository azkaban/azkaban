---
layout: documents
nav: executingflow
context: ../..
---

#Executing Flow

From the [Flow View](./projectflowview.html) or the Project Page, you can trigger a job to be executed.
You will see an executing panel pop-up.

<hr/>

##Executing Flow View
From the Flow View panel, you can right click on the graph and disable or enable jobs. Disabled jobs will be skipped during execution
as if their dependencies have been met. Disabled jobs will appear translucent.

<img class="shadowimg" title="Executing Flow View" src="./images/executeflowpanel.png" ALT="Executing Flow View" width="450" />

<hr/>

##Notification Options
The notification options allow users to change the flow's success or failure notification behavior.

####Notify on Failure
* __First Failure__ - Send failure emails after the first failure is detected.
* __Flow Finished__ - If the flow has a job that has failed, it will send failure emails after all jobs in the flow have finished.

####Email overrides
Azkaban will use the default notification emails set in the final job in the flow. If overridden, a user can change
the email addresses where failure or success emails are sent. The list can be delimited by commas, whitespace or a semi-colon.

<img class="shadowimg" title="Notification Options" src="./images/executeflownotify.png" ALT="Notification Options" width="450" />

<hr/>

##Failure Options
When a job in a flow fails, you are able to control how the rest of the flow will succeed.

* __Finish Current Running__ will finish the jobs that are currently running, but it will not start new jobs. The flow will be put in the _FAILED FINISHING_ state and be set to FAILED once everything completes.
* __Cancel All__ will immediately kill all running jobs and set the state of the executing flow to FAILED.
* __Finish All Possible__ will keep executing jobs in the flow as long as its dependencies are met. The flow will be put in the _FAILED FINISHING_ state and be set to FAILED once everything completes.

<img class="shadowimg" title="Failure Options" src="./images/executeflowfailure.png" ALT="Failure Options" width="450" />

<hr/>

##Concurrent Options
If the flow execution is invoked while the flow is concurrently executing, several options can be set.

* __Skip Execution__ option will not run the flow if its already running.
* __Run Concurrently__ option will run the flow regardless of if its running. Executions are given different working directories.
* __Pipeline__ runs the the flow in a manner that the new execution will not overrun the concurrent execution.
    * Level 1 : blocks executing __job A__ until the the previous flow's __job A__ has completed.
    * Level 2 : blocks executing __job A__ until the the children of the previous flow's __job A__ has completed. This is useful if you need to run your flows a few steps behind an already executing flow.

<img class="shadowimg" title="Concurrent Options" src="./images/executeflowconcurrent.png" ALT="Concurrent Options" width="450" />

<hr/>

##Flow Parameters
Allows users to override flow parameters. The flow parameters override properties that jobs inherit from shared files (specified in _.properties_ files), but will not override properties set in the _.job_ file itself.

<img class="shadowimg" title="Flow Parameters Options" src="./images/executeflowparameters.png" ALT="Flow Parameters" width="450" />
