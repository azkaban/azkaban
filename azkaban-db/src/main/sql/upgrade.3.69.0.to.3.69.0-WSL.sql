-- TODO DB Migration from release 3.69.0 to 3.69.0-WSL
-- PR #???? Implement "executor tags" feature for new dispatching Logic (Poll model).
--
CREATE TABLE executor_tags (
  executor_id INT         NOT NULL REFERENCES executors (id),
  tag         VARCHAR(64) NOT NULL,
  PRIMARY KEY (executor_id, tag)
);

CREATE TABLE project_flow_required_tags (
  project_id    INT           NOT NULL,
  version       INT           NOT NULL,
  flow_id       VARCHAR(128),
  tag           VARCHAR(64)   NOT NULL,
  PRIMARY KEY (project_id, version, flow_id, tag),
  FOREIGN KEY (project_id, version, flow_id) REFERENCES project_flows (project_id, version, flow_id)
);
