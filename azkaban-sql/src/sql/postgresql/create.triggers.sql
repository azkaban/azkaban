CREATE TABLE triggers (
	trigger_id SERIAL NOT NULL PRIMARY KEY,
	trigger_source VARCHAR(128),
	modify_time BIGINT NOT NULL,
	enc_type SMALLINT,
	data BYTEA
);
