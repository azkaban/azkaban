package azkaban.jobtype.connectors.gobblin.helper;

import azkaban.jobtype.connectors.gobblin.GobblinConstants;
import azkaban.jobtype.javautils.ValidationUtils;
import azkaban.utils.Props;

public class HdfsToMySqlValidator implements IPropertiesValidator {

  @Override
  public void validate(Props props) {
    ValidationUtils.validateAllNotEmpty(props
        , GobblinConstants.GOBBLIN_WORK_DIRECTORY_KEY
        , "jdbc.publisher.database_name" //Database
        , "jdbc.publisher.table_name"    //Table
        , "jdbc.publisher.username"
        , "jdbc.publisher.password"
        , "jdbc.publisher.url"
        , "extract.table.type" //snapshot_only, append_only, snapshot_append
        , "source.filebased.data.directory");
  }
}
