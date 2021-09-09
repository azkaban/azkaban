-- DB Migration from release 3.90.0 to 4.0.0
-- Adding dispatch_method column in execution_flows
ALTER TABLE execution_flows ADD COLUMN dispatch_method TINYINT DEFAULT 1;

CREATE INDEX ex_flows_dispatch_method ON execution_flows (dispatch_method);
