-- Definition for image_types table. This table is used for storing different image types
CREATE TABLE IF NOT EXISTS image_types (
  id               INT             NOT NULL PRIMARY KEY AUTO_INCREMENT,
  name             VARCHAR(64)     NOT NULL UNIQUE,
  description      VARCHAR(2048),
  active           BOOLEAN,
  deployable       VARCHAR(64),
  created_on       TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by       VARCHAR(64)     NOT NULL,
  modified_on      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by      VARCHAR(64)     NOT NULL
);

-- TODO: create index if not exists is not supported. Hence, current Azkaban codebase throws
--  duplicate index exception during build. This to be addressed separately. Commenting it for
--  now. One option is to move each table create scripts to separate file. But all the
--  containerization tables are placed in this file so that it easier to manage.

-- Index on image_types table.
-- create index image_type_name
-- on image_types (name);

-- create index active_image_type
-- on image_types (active);

-- Definition for image_versions table. This table is used for storing versions of an image type
CREATE TABLE IF NOT EXISTS image_versions (
  id               INT             NOT NULL PRIMARY KEY AUTO_INCREMENT,
  path             VARCHAR(1024)   NOT NULL,
  description      VARCHAR(2048),
  version          VARCHAR(64)     NOT NULL,
  type_id          INT NOT NULL,   FOREIGN KEY(type_id) references image_types (id),
  state            VARCHAR(64)     NOT NULL,
  release_tag      VARCHAR(64)     NOT NULL,
  created_on       TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by       VARCHAR(64)     NOT NULL,
  modified_on      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by      VARCHAR(64)     NOT NULL,
  UNIQUE (type_id, version)
);

-- Definition for image_ownerships table. This table is used for storing ownership information for
-- an image type
CREATE TABLE IF NOT EXISTS image_ownerships (
  id               INT             NOT NULL PRIMARY KEY AUTO_INCREMENT,
  type_id          INT NOT NULL,   FOREIGN KEY(type_id) references image_types (id),
  owner            VARCHAR(64)     NOT NULL,
  role             VARCHAR(64)     NOT NULL,
  created_on       TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by       VARCHAR(64)     NOT NULL,
  modified_on      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by      VARCHAR(64)     NOT NULL
);

-- Definition for image_rampup_plan table. This table is used for creating rampup plan for an
-- image type. Only one ramp up plan will be active at a time.
CREATE TABLE IF NOT EXISTS image_rampup_plan (
  id               INT             NOT NULL PRIMARY KEY AUTO_INCREMENT,
  name             VARCHAR(1024)   NOT NULL,
  description      VARCHAR(2048),
  type_id          INT NOT NULL,   FOREIGN KEY(type_id) references image_types (id),
  active           BOOLEAN         NOT NULL,
  created_on       TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by       VARCHAR(64)     NOT NULL,
  modified_on      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by      VARCHAR(64)     NOT NULL
);

-- TODO: create index if not exists is not supported. Hence, current Azkaban codebase throws
--  duplicate index exception during build. This to be addressed separately. Commenting it for now.
--  One option is to move each table create scripts to separate file. But all the  containerization
--  tables are placed in this file so that it easier to manage.

-- Index on image_rampup_plan table
-- create index active_rampup_plan
-- on image_rampup_plan (active);

-- Definition for image_rampup table. This table contains information of the image versions being
-- ramped up for an image type
CREATE TABLE IF NOT EXISTS image_rampup (
  id                INT            NOT NULL PRIMARY KEY AUTO_INCREMENT,
  plan_id           INT            NOT NULL, FOREIGN KEY(plan_id) references image_rampup_plan (id),
  version_id        INT            NOT NULL, FOREIGN KEY(version_id) references image_versions (id),
  rampup_percentage INT            NOT NULL DEFAULT 0,
  stability_tag     VARCHAR(64)    NOT NULL,
  created_on        TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by        VARCHAR(64)    NOT NULL,
  modified_on       TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by       VARCHAR(64)    NOT NULL
);

-- Definition for version_set table. Version set contains set of image versions and will be
-- used during flow container launch
CREATE TABLE IF NOT EXISTS version_set (
     id  INT NOT NULL AUTO_INCREMENT,
     md5  CHAR(32) NOT NULL,
     json VARCHAR(4096) NOT NULL,
     created_on datetime DEFAULT CURRENT_TIMESTAMP,
     PRIMARY KEY (id)
);

CREATE UNIQUE INDEX idx_md5 ON version_set (md5);

-- TODO: Add the alter table script in the specific release
-- Adding image_version_set_id column in execution_flows
-- alter table execution_flows add column image_version_set_id INT default null;
