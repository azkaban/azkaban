CREATE TABLE execution_dependencies(
  trigger_instance_id varchar(64),
  dep_name varchar(128),
  starttime datetime not null,
  endtime datetime,
  dep_status tinyint not null,
  cancelleation_cause tinyint not null,

  project_id INT not null,
  project_version INT not null,
  flow_id varchar(128) not null,
  flow_version INT not null,
  project_json MEDIUMTEXT not null,
  flow_exec_id INT not null,
  primary key(trigger_instance_id, dep_name)
);