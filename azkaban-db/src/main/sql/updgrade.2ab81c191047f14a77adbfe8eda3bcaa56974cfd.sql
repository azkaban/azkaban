-- TODO what are the from & to versions for this?
-- the original change of create.execution_flows.sql was done in commit:
-- 2ab81c191047f14a77adbfe8eda3bcaa56974cfd

-- DB Migration from release ??? to ???
-- Adding dispatch_method column in execution_flows
ALTER TABLE execution_flows ADD COLUMN dispatch_method TINYINT DEFAULT 1;

CREATE INDEX ex_flows_dispatch_method ON execution_flows (dispatch_method);
