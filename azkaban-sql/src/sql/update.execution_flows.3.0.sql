ALTER TABLE execution_flows ADD COLUMN executor_id INT DEFAULT NULL;
CREATE INDEX executor_id ON execution_flows(executor_id);