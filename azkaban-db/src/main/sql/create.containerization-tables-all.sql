CREATE TABLE IF NOT EXISTS image_types (
  id               INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
  type             VARCHAR(64) NOT NULL UNIQUE,
  description      VARCHAR(2048),
  active           BOOLEAN,
  deployable       VARCHAR(64),
  created_on       TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by       VARCHAR(64)     NOT NULL,
  modified_on      TIMESTAMP       DEFAULT NULL,
  modified_by      VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS image_versions (
  id               INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
  path             VARCHAR(1024) NOT NULL,
  description      VARCHAR(2048),
  version          VARCHAR(64) NOT NULL,
  type_id          INT NOT NULL, FOREIGN KEY(type_id) references image_types (id),
  state            VARCHAR(64) NOT NULL,
  release_tag      VARCHAR(64) NOT NULL,
  created_on       TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by       VARCHAR(64)     NOT NULL,
  modified_on      TIMESTAMP       DEFAULT NULL,
  modified_by      VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS image_ownerships (
  id               INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
  type_id          INT NOT NULL, FOREIGN KEY(type_id) references image_types (id),
  owner            VARCHAR(64) NOT NULL,
  role             VARCHAR(64) NOT NULL,
  created_on       TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by       VARCHAR(64)     NOT NULL,
  modified_on      TIMESTAMP       DEFAULT NULL,
  modified_by      VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS image_rampup_plan (
  id               INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
  name             VARCHAR(1024) NOT NULL,
  description      VARCHAR(2048),
  type_id          INT NOT NULL, FOREIGN KEY(type_id) references image_types (id),
  active           BOOLEAN NOT NULL,
  created_on       TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by       VARCHAR(64)     NOT NULL,
  modified_on      TIMESTAMP       DEFAULT NULL,
  modified_by      VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS image_rampup (
  id                INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
  plan_id           INT NOT NULL, FOREIGN KEY(plan_id) references image_rampup_plan (id),
  version_id        INT NOT NULL, FOREIGN KEY(version_id) references image_versions (id),
  rampup_percentage INT NOT NULL DEFAULT 0,
  stability_tag     VARCHAR(64)     NOT NULL,
  created_on        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by        VARCHAR(64)     NOT NULL,
  modified_on       TIMESTAMP       DEFAULT NULL,
  modified_by       VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS image_version_set (
  id               INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
  version_set      blob NOT NULL,
  version_set_hash binary(16) NOT NULL UNIQUE,
  created_on       TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by       VARCHAR(64)     NOT NULL
);

-- Adding image_version_set_id column in execution_flows
alter table execution_flows add column image_version_set_id INT default null;
