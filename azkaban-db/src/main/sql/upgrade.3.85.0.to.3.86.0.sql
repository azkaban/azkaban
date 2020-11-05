-- DB Migration from release 3.85.0 to 3.86.0
-- Adding ExecutionSource column in execution_flows
alter table execution_flows add column execution_source varchar(32) default null;
