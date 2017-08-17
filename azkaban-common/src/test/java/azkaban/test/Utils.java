package azkaban.test;

import static azkaban.ServiceProvider.SERVICE_PROVIDER;

import azkaban.database.AzkabanDatabaseSetup;
import azkaban.db.AzDBTestUtility.EmbeddedH2BasicDataSource;
import azkaban.db.AzkabanDataSource;
import azkaban.db.DatabaseOperator;
import azkaban.db.DatabaseOperatorImpl;
import azkaban.utils.Props;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.File;
import org.apache.commons.dbutils.QueryRunner;


public class Utils {

  public static void initServiceProvider() {
    final Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
      }
    });
    // Because SERVICE_PROVIDER is a singleton and it is shared among many tests,
    // need to reset the state to avoid assertion failures.
    SERVICE_PROVIDER.unsetInjector();

    SERVICE_PROVIDER.setInjector(injector);
  }

  public static DatabaseOperator initTestDB() throws Exception{
    final AzkabanDataSource dataSource = new EmbeddedH2BasicDataSource();

    final String sqlScriptsDir = new File("../azkaban-db/src/main/sql/").getCanonicalPath();
    final Props props = new Props();
    props.put("database.sql.scripts.dir", sqlScriptsDir);

    // TODO kunkun-tang: Need to refactor AzkabanDatabaseSetup to accept datasource in azkaban-db
    final azkaban.database.AzkabanDataSource dataSourceForSetupDB =
        new azkaban.database.AzkabanConnectionPoolTest.EmbeddedH2BasicDataSource();
    final AzkabanDatabaseSetup setup = new AzkabanDatabaseSetup(dataSourceForSetupDB, props);
    setup.loadTableInfo();
    setup.updateDatabase(true, false);

    return new DatabaseOperatorImpl(new QueryRunner(dataSource));
  }
}
