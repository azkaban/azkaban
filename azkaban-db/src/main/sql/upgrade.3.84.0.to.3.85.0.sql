-- DB Migration from release 3.84.0 to 3.85.0
-- Adding Project Uploader's IP Address in project_versions
alter table project_versions add column uploader_ip_addr varchar(50) default null;
