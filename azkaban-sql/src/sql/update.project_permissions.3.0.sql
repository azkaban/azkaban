ALTER TABLE project_permissions drop PRIMARY KEY, add PRIMARY KEY(project_id, name,isGroup);
