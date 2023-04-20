CREATE TABLE ramp_rules (
    rule_name VARCHAR(100) NOT NULL,
    image_name VARCHAR(200),
    image_version VARCHAR (100),
    owners VARCHAR (1000) NOT NULL,
    is_HP BIT NOT NULL,
    created_by VARCHAR(20) NOT NULL,
    created_on DATE NOT NULL,
    modified_by VARCHAR(20) NOT NULL,
    modified_on DATE NOT NULL,
    PRIMARY KEY (rule_name)
);

CREATE INDEX idx_rule_name
    ON ramp_rules (rule_name);