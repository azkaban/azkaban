CREATE TABLE flow_deny_lists (
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    flow_id VARCHAR(100) NOT NULL,
    deny_mode VARCHAR(10) NOT NULL ,
    deny_version VARCHAR (100),
    rule_name VARCHAR (100) NOT NULL
);

CREATE INDEX idx_flow_id
    ON flow_deny_lists (flow_id);
CREATE INDEX idx_flow_deny_rule_name
    ON flow_deny_lists (rule_name);
CREATE INDEX idx_flow_id_deny_mode
    ON flow_deny_lists (flow_id, deny_mode);
CREATE INDEX idx_flow_id_deny_version
    ON flow_deny_lists (flow_id, deny_version);