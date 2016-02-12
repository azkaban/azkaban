package azkaban.execapp.cluster;

import azkaban.event.EventListener;
import azkaban.executor.ExecutableFlow;
import org.apache.log4j.Logger;


/**
 * Created by jsoumet on 2/7/16 for azkaban.
 */
public interface IClusterManager extends EventListener {


    /**
     * Is passed a flow and logger. Returns true if Cluster preparation is successful, false if error.
     * @param flow
     * @param logger
     * @return
     */
    boolean createClusterAndConfigureJob(ExecutableFlow flow, Logger logger);


    /**
     * Optionally terminate a cluster after the flow is completed.
     * @param flow
     * @param logger
     */
    void maybeTerminateCluster(ExecutableFlow flow, Logger logger);


}