package azkaban.cluster;

import azkaban.execapp.AzkabanExecutorServer;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import java.io.File;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class ClusterModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ClusterLoader.class).asEagerSingleton();
    bind(ClusterRegistry.class);
  }

  @Provides
  @Named("clusterDir")
  public File getClusterDir(final Props props) {
    final String clusterDir = props.getString(AzkabanExecutorServer.CLUSTER_CONFIG_DIR, "cluster");
    return new File(clusterDir);
  }

  @Provides
  public ClusterRouter getClusterRouter(final Props props, final ClusterRegistry clusterRegistry,
      final Configuration conf)
      throws ClassNotFoundException {
    final String routerClass = props.getString(AzkabanExecutorServer.CLUSTER_ROUTER_CLASS,
        DisabledClusterRouter.class.getName());
    final Class<?> routerClazz = Class.forName(routerClass);
    return (ClusterRouter) Utils.callConstructor(routerClazz, clusterRegistry, conf);
  }

  @Provides
  @Singleton
  public Configuration getRouterConf(final Props props) {
    // TODO: put necessary core-site properties into robin-site.xml and do not load default xmls
    final Configuration configuration = new Configuration();
    for (Map.Entry<String, String> entry : props.getFlattened().entrySet()) {
      configuration.set(entry.getKey(), entry.getValue());
    }
    final String routerConfPath = props.getString(AzkabanExecutorServer.CLUSTER_ROUTER_CONF,
        "router-conf.xml");
    configuration.addResource(new Path(routerConfPath));
    return configuration;
  }
}
