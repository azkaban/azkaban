---
layout: documents
nav: generictrigger
context: ../..
---
# Generic Trigger

Introduced in release-2.2, generic trigger is another plugin system that will introduce new features into Azkaban.

## Tiggers, Conditions, Checkers, and Actions

A trigger comes with two conditions: a trigger condition and an expire condition. When a trigger condition is met, Azkaban trigger manager will invoke a set of pre-set actions. The condition has expressions such as "timechecker1.eval()", in which _timechecker1_ simply checks if a time point has arrived.

<br/>
In our implementation, the checkers are plugins. It can check on time, on flow execution, on data availability in Hadoop, or any internal/external events.
<br/>
The condition expressions can be complex and rely on multiple checkers, for instance "(timechecker1.eval() && datachecker1.eval()) || httpeventchecker1.eval()".
<br/>
The actions are also plugins. One can invoke a flow execution, send out alert (also plugin), or create another generic trigger in the system.

<br/>
With generic triggers, we should be able to easily introduce new trigger methods. For example, we can have more smart time based scheduling; we can have data based triggers that can start flows based on data availability from different data sources, etc.

In release-2.2, we have moved time based scheduling and sla manager into trigger system.