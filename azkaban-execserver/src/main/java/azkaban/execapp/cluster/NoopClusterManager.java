package azkaban.execapp.cluster;

import azkaban.event.Event;
import azkaban.executor.ExecutableFlow;
import org.apache.log4j.Logger;

/**
 * Created by jsoumet on 2/7/16 for azkaban.
 */
public class NoopClusterManager implements IClusterManager {


    @Override
    public boolean createClusterAndConfigureJob(ExecutableFlow flow, Logger logger) {
        return true;
    }

    @Override
    public void maybeTerminateCluster(ExecutableFlow flow, Logger logger) {

    }

    @Override
    public void handleEvent(Event event) {

    }
}
