---
layout: azkaban
nav: home
---

#Azkaban

Azkaban is a batch workflow job scheduler created at LinkedIn to run their Hadoop Jobs.

Often times there is a need to run a set of jobs and processes in a particular order within a 
workflow.  Azkaban will resolve the ordering through job dependencies and provide an easy to use 
web user interface to maintain and track your workflows. Here are a few features:

<div style="float: right;"><img title="Azkaban Flow Viewer" src="images/azkabanshot.png" ALT="Azkaban UI" width="333" height="246" /></div>
* Compatible with any version of Hadoop
* Easy to use web UI
* Simple web and http workflow uploads
* Project workspaces
* Scheduling of workflows
* Modular and pluginable
* Authentication and Authorization
* Tracking of user actions
* Email alerts on failure and successes
* SLA alerting and auto killing
* Retrying of failed jobs
<br/>
<br/>
<br/>

Although there are a lot of other workflow scheduler solutions in existance, Azkaban was designed 
primarily with usability in mind. It has been running at LinkedIn for several years, and drives
many of their Hadoop and data warehouse processes.