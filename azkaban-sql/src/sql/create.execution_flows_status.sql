-- -----------------------------------------------------------------
-- Create lookup table for 'status' values in execution_flows table
-- -----------------------------------------------------------------
CREATE TABLE if NOT EXISTS execution_flows_status (
	name VARCHAR(64),
	status INT,
	PRIMARY KEY (name)
);
-- -----------------------------------------------------------------
-- Populate enum values into lookup table
-- -----------------------------------------------------------------
INSERT INTO execution_flows_status (name, status) VALUES 
  ("READY",10),
  ("PREPARING",20),
  ("RUNNING",30),
  ("PAUSED",40),
  ("SUCCEEDED",50),
  ("KILLED",60),
  ("FAILED",70),
  ("FAILED_FINISHING",80),
  ("SKIPPED",90),
  ("DISABLED",100),
  ("QUEUED",110),
  ("FAILED_SUCCEEDED",120),
  ("CANCELED",130);
