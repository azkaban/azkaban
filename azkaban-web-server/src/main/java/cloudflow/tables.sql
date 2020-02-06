CREATE TABLE IF NOT EXISTS `space` (
                                       `id` int(11) NOT NULL AUTO_INCREMENT,
                                       `name` varchar(50) NOT NULL,
                                       `description` varchar(300) NOT NULL,
                                       `created_on` datetime NOT NULL,
                                       `created_by` varchar(45) NOT NULL,
                                       `modified_on` datetime NOT NULL,
                                       `modified_by` varchar(45) NOT NULL,
                                       PRIMARY KEY (`id`)

);

CREATE TABLE IF NOT EXISTS `space_admin` (
                                             `space_id` int(11) NOT NULL,
                                             `username` varchar(45) NOT NULL,
                                             PRIMARY KEY (`space_id`,`username`)
);

CREATE TABLE IF NOT EXISTS `space_watcher` (
                                               `space_id` int(11) NOT NULL,
                                               `username` varchar(45) NOT NULL,
                                               PRIMARY KEY (`space_id`,`username`)
);

/*
Run these statements to add the new columns to existing table
ALTER TABLE projects
ADD COLUMN space_id INT,
ADD COLUMN created_by VARCHAR(64);
*/

CREATE TABLE IF NOT EXISTS `project_admin` (
 project_id   INT(11)      NOT NULL,
 username     VARCHAR(64)  NOT NULL,
 PRIMARY KEY (`project_id`,`username`)
);


-- Required modifications of Azkaban's DB schema to support executions endpoints
-- ALTER TABLE execution_flows ADD COLUMN experiment_id INT DEFAULT NULL;
-- ALTER TABLE execution_flows ADD COLUMN flow_definition_id INT;
-- ALTER TABLE execution_flows ADD COLUMN flow_version INT;

-- ***********************
-- Required modifications of Azkaban's DB schema to support GET /flows endpoints
-- ALTER TABLE project_flows
-- ADD COLUMN flow_name VARCHAR(512),
-- ADD COLUMN latest_flow_version INT;

-- ALTER TABLE project_flow_files
-- ADD COLUMN flow_id INT,
-- ADD COLUMN experimental TINYINT(1),
-- ADD COLUMN dsl_version TINYINT(4),
-- ADD COLUMN created_by VARCHAR(64),
-- ADD COLUMN locked TINYINT(1),
-- ADD COLUMN encoding_type TINYINT(4),
-- ADD COLUMN flow_model_blob BLOB;
-- ***********************
