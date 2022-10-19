CREATE TABLE project_flow_recommendations (
  id                    INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
  project_id            INT         NOT NULL,
  flow_id               VARCHAR(128),
  modified_time         BIGINT      NOT NULL,
  cpu_recommendation    VARCHAR(128),
  memory_recommendation VARCHAR(128),
  disk_recommendation   VARCHAR(128)
);

CREATE INDEX flow_recommendation_project
  ON project_flow_recommendations (project_id);

CREATE UNIQUE INDEX flow_recommendation_project_flow
  ON project_flow_recommendations (project_id, flow_id);