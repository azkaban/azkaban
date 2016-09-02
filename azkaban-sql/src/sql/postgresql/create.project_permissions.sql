CREATE TABLE project_permissions (
  project_id INT NOT NULL,
  modified_time bigint NOT NULL,
  name character varying(64) NOT NULL,
  permissions integer NOT NULL,
  isgroup boolean NOT NULL,
  CONSTRAINT project_permissions_pkey PRIMARY KEY (project_id, name),
  CONSTRAINT project_permission_project_id UNIQUE (project_id)
);

CREATE INDEX permission_index ON project_permissions(project_id);
