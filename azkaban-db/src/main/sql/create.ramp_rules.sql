CREATE TABLE ramp_rules (
    rule_id VARCHAR(100) NOT NULL,
    image_name VARCHAR(200) NOT NULL,
    image_version VARCHAR (100) NOT NULL,
    owners VARCHAR (1000) NOT NULL,
    is_HP BIT NOT NULL,
    created_by VARCHAR(20) NOT NULL,
    created_on DATE NOT NULL,
    modified_by VARCHAR(20),
    modified_on DATE,
    PRIMARY KEY (rule_id)
);

CREATE INDEX idx_rule_id
    ON ramp_rules (rule_id);