CREATE TABLE version_set
  (
     id  INT NOT NULL AUTO_INCREMENT,
     md5  CHAR(32) NOT NULL,
     json VARCHAR(4096) NOT NULL,
     created_on datetime DEFAULT CURRENT_TIMESTAMP,
     PRIMARY KEY (id)
  );

CREATE UNIQUE INDEX idx_md5 ON version_set (md5);
