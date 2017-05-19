package azkaban.execapp.fake;

import azkaban.execapp.AzkabanExecutorServer;
import azkaban.execapp.jmx.JmxJobMBeanManager;
import azkaban.utils.Props;

public class FakeApp extends AzkabanExecutorServer {

    public FakeApp() throws Exception {
        super(null, null, new Props(), new FakeServer());
        JmxJobMBeanManager.getInstance().initialize(new Props());
    }

}
