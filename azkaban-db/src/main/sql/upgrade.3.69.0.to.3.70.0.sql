-- DB Migration from release 3.69.0 to 3.70.0
-- PR #2140 Implement “flowPriority” (at the time of polling) feature for new dispatching logic.
-- flow_priority column can hold a positive int, negative int or 5 which is the default flow
-- priority set in ExecutionOptions.DEFAULT_FLOW_PRIORITY for all flows.
-- "flowPriority" is an execution option an Azkaban admin can specify when launching a new execution.
-- It will allow a flow to be dispatched or polled first.
--
ALTER TABLE execution_flows ADD COLUMN flow_priority INT NOT NULL DEFAULT 5;
