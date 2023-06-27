DROP PROCEDURE IF EXISTS UpdateTableIfColumnNotPresent;

DELIMITER $$

CREATE PROCEDURE UpdateTableIfColumnNotPresent(
    IN tableName VARCHAR(255),
    IN columnName VARCHAR(255),
    IN variableType VARCHAR(50),
    IN defaultValue VARCHAR(50)
)
BEGIN
	DECLARE _COUNT INT;
    SELECT COUNT(*) INTO _COUNT FROM INFORMATION_SCHEMA.COLUMNS WHERE
        table_schema=DATABASE() and table_name = tableName and column_name = columnName;
    IF (_COUNT <= 0) THEN
        set @update = CONCAT("ALTER TABLE ", tableName, " ADD COLUMN ", columnName, " ", variableType, " default ", defaultValue, ";");
        SELECT CONCAT("THE RUNNING COMMAND IS ", "ALTER TABLE ", tableName, " ADD COLUMN ", columnName, " ", variableType, " default ", defaultValue, ";");
        PREPARE stmt FROM @update;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    ELSE
        SELECT CONCAT("COLUMN ", columnName, " ALREADY EXISTS ON TABLE ", tableName);
    END IF;
END$$

DELIMITER ;

CALL UpdateTableIfColumnNotPresent("execution_flows","version_set_id", "INT", "null");

DROP PROCEDURE IF EXISTS UpdateTableIfColumnNotPresent;


