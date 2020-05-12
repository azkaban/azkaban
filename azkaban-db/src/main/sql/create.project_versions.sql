CREATE TABLE project_versions (
  project_id           INT           NOT NULL,
  version              INT           NOT NULL,
  upload_time          BIGINT        NOT NULL,
  uploader             VARCHAR(64)   NOT NULL,
  file_type            VARCHAR(16),
  file_name            VARCHAR(128),
  md5                  BINARY(16),
  num_chunks           INT,
  resource_id          VARCHAR(512)  DEFAULT NULL,
  startup_dependencies MEDIUMBLOB    DEFAULT NULL,
  uploader_ip_addr     VARCHAR(50)   DEFAULT NULL,
  PRIMARY KEY (project_id, version)
);

CREATE INDEX version_index
  ON project_versions (project_id);
