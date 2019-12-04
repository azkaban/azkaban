CREATE TABLE validated_dependencies (
  file_name         VARCHAR(128),
  file_sha1         CHAR(40),
  validation_key    CHAR(40),
  validation_status INT,
  PRIMARY KEY (validation_key, file_name, file_sha1)
);
