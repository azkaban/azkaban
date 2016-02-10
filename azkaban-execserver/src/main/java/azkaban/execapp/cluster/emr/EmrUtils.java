package azkaban.execapp.cluster.emr;

import azkaban.utils.Props;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by jsoumet on 2/8/16 for azkaban.
 */
public class EmrUtils {



    private Cluster findClusterById(AmazonElasticMapReduceClient emrClient, String clusterId) {
        DescribeClusterRequest descRequest = new DescribeClusterRequest().withClusterId(clusterId);
        try {
            DescribeClusterResult clusterDescription = emrClient.describeCluster(descRequest);
            return clusterDescription.getCluster();
        } catch (InvalidRequestException e) {
            throw new RuntimeException("Error looking for cluster " + clusterId, e);
        }
    }

    private ClusterSummary findClusterByName(AmazonElasticMapReduceClient emrClient, String clusterName) {
        return findClusterByName(emrClient, clusterName, null, null);
    }

    private ClusterSummary findClusterByName(AmazonElasticMapReduceClient emrClient, String clusterName, Collection<String> clusterStates) {
        return findClusterByName(emrClient, clusterName, clusterStates, null);
    }

    private ClusterSummary findClusterByName(AmazonElasticMapReduceClient emrClient, String clusterName, Set<ClusterState> clusterStates) {
        return findClusterByName(emrClient, clusterName, clusterStates.stream().map(ClusterState::toString).collect(Collectors.toList()));
    }

    private ClusterSummary findClusterByName(AmazonElasticMapReduceClient emrClient, String clusterName, Collection<String> clusterStates, String marker) {
        ListClustersRequest req = new ListClustersRequest();
        if (marker != null) req.setMarker(marker);
        if (clusterStates != null) req.setClusterStates(clusterStates);

        ListClustersResult res = emrClient.listClusters(req);
        for (ClusterSummary cluster : res.getClusters()) {
            if (clusterName.equals(cluster.getName())) {
                return cluster;
            }
        }

        if (res.getClusters().size() == 50) { // TODO: use a different method to detect if more results
            return findClusterByName(emrClient, clusterName, clusterStates, res.getMarker());
        }
        return null;
    }

    private Cluster describeCluster(AmazonElasticMapReduceClient emrClient, String clusterId) {
        return emrClient.describeCluster(new DescribeClusterRequest().withClusterId(clusterId)).getCluster();
    }


    public void blockUntilClusterIsReady() throws InterruptedException, IOException {
        int waitMinutes = CLUSTER_READINESS_TIMEOUT;
        while (--waitMinutes > 0) {
            if (isClusterReady()) {
                // cluster is ready.. update config files if necessary
                log.info("Cluster is ready!");
                updateConfigFilesWithMasterIp();
                return;
            }
            log.info("Waiting for cluster " + clusterId + " to be ready...");
            Thread.sleep(60000);
        }
        throw new RuntimeException("Failed to connect to cluster " + clusterId + " after " + CLUSTER_READINESS_TIMEOUT + " minutes.");
    }

    public String getNewHadoopConfigurationDir() {
        if (newConfigurationDir == null) throw new RuntimeException("Tried to get hadoop conf dir but cluster was not initialized.");
        return newConfigurationDir;
    }
    public String getNewClassPath() {
        if (newClassPath == null) throw new RuntimeException("Tried to get class path but cluster was not initialized.");
        return newClassPath;
    }


    public void initialize(Props configProps, String oldGlobalClasspath) throws IOException, InterruptedException {
        String originalConfigurationDir = configProps.getString("hadoop.conf.dir");
        newConfigurationDir = configProps.getString("working.dir") + "/conf";
        newClassPath = appendToClassPathString(oldGlobalClasspath, newConfigurationDir);
        log.info("Emr configuration initialization");
        // Cluster naming convention
        clusterName = computeClusterNameFromProps(configProps);

        if ((new File(newConfigurationDir)).exists()) { // another job in the flow already set-up the configuration
            log.info("Configuration folder already initialized by another job. Will skip EMR cluster management.");
            return ;
        }

        synchronized (EmrClusterController.class) { // lock to prevent race condition
            if (configProps.containsKey(EMR_CLUSTER_NEW_PROP) && configProps.getBoolean(EMR_CLUSTER_NEW_PROP, false)) {
                log.info("Creating a new EMR cluster...");
                createNewCluster(configProps, clusterName);
                createConfigFiles(originalConfigurationDir);
                return;
            }

            if (configProps.containsKey(EMR_CLUSTER_ID_PROP)) {
                String clusterId = configProps.getString(EMR_CLUSTER_ID_PROP);
                log.info("Specific EMR cluster requested: " + clusterId);
                configureByClusterId(clusterId);
                if (isClusterActive()) {
                    log.info("Found cluster " + clusterId);
                } else {
                    throw new RuntimeException("Could not find cluster " + clusterId);
                }
                createConfigFiles(originalConfigurationDir);
                return ;
            }

            if (configProps.containsKey(EMR_CLUSTER_DEFAULT_PROP)) {
                this.log.info("Will run on default persistent EMR cluster");
                //TODO: find clusterId for current long-running cluster.
                //TODO: this could mean we dont have to restart the azk docker when using a new cluster
                //TODO: but need to think about this feature first
                //TODO: for now just return the old conf path
                //createConfigFiles(originalConfigurationDir, newConfigurationDir, getMasterIP());
                //return newClassPath;
                newClassPath = oldGlobalClasspath;
                return;
            }
        }
        return;
    }


    public String getMasterIP() {
        if (clusterId != null) {
            ListInstancesRequest req = new ListInstancesRequest().withClusterId(clusterId)
                    .withInstanceGroupTypes(InstanceGroupType.MASTER);
            ListInstancesResult res = client.listInstances(req);
            if (res.getInstances().size() > 0) {
                return res.getInstances().get(0).getPrivateIpAddress();
            }
        } else {
            throw new RuntimeException("Called getMasterIP but did not provide clusterId");
        }
        throw new RuntimeException("Called getMasterIP but could not find that cluster. ClusterId=" + clusterId);
    }

    public void terminateCluster() {
        if (clusterId != null) {
            log.info("Terminating cluster " + clusterId);
            client.terminateJobFlows(new TerminateJobFlowsRequest(Collections.singletonList(clusterId)));
        } else {
            throw new RuntimeException("Attempted to terminate cluster before initializing.");
        }
    }


    public void maybeTerminateCluster(Props jobProps, boolean jobErrored) {
        if (jobErrored && jobProps.getBoolean(EMR_CONF_CLUSTER_TERMINATE_ON_ERROR, EMR_DEFAULT_CLUSTER_TERMINATE_ON_ERROR)) {
            log.info("Terminating cluster due to job errors.");
            terminateCluster();
        } else if (jobProps.getBoolean(EMR_CONF_CLUSTER_TERMINATE_ON_COMPLETION, EMR_DEFAULT_CLUSTER_TERMINATE_ON_COMPLETION)) {
            terminateCluster();
        } else {
            log.info("Not terminating cluster");
        }
    }

    /**
     * Call EMR api to find out if cluster is in running state
     */
    public boolean isClusterActive() {
        if (clusterId != null) {
            Cluster cluster = findClusterById(this.clusterId);
            if (RUNNING_STATES.contains(ClusterState.fromValue(cluster.getStatus().getState()))) {
                return true;
            }
        } else {
            throw new RuntimeException("Called isClusterActive but clusterId was null.");
        }
        return false;
    }

    public boolean isClusterReady() {
        synchronized (EmrClusterController.class) {
            try {
                // sleep for random amount of time. max 5 seconds. To avoid hitting the EMR api too frequently
                Thread.sleep((long)(Math.random() * 5000));
            } catch (InterruptedException e) {
                // ignore
            }

            if (clusterId == null) try {
                configureFromClusterName(clusterName);
            } catch (InterruptedException e) {
                throw new RuntimeException("Could not find cluster.");
            }
            if (clusterId != null) {
                Cluster cluster = findClusterById(this.clusterId);
                if (READY_STATES.contains(ClusterState.fromValue(cluster.getStatus().getState()))) {
                    return true;
                } else if (!RUNNING_STATES.contains(ClusterState.fromValue(cluster.getStatus().getState()))) {
                    throw new RuntimeException("Called isClusterReady but cluster was terminated.");
                }
            } else {
                throw new RuntimeException("Called isClusterReady but clusterId was null.");
            }
            return false;
        }
    }


    /**
     * Create executions/xx/conf folder so that Yarn can pick up the new cluster
     * Synchronized so 2 instances dont try to create the folder at the same time
     */
    private void createConfigFiles(String oldConfPath) {
        try {
            log.info("Creating config files in " + newConfigurationDir);
            FileUtils.copyDirectory(new File(oldConfPath), new File(newConfigurationDir));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create configuration folder.", e);
        }
    }

    /**
     * Called when cluster is available. will modify once
     */
    private void updateConfigFilesWithMasterIp() throws IOException {
        // replace old ip with new one
        Configuration configuration = AmazonUtils.loadHadoopConfig(newConfigurationDir);
        String oldMasterIp = configuration.get("hadoop.master.ip");
        String newMasterIp = getMasterIP(); // from EMR

        if (!oldMasterIp.equals(newMasterIp)) { // maybe we already changed it
            String coreSiteXml = new String(Files.readAllBytes(Paths.get(newConfigurationDir, "core-site.xml")), StandardCharsets.UTF_8);
            coreSiteXml = coreSiteXml.replaceAll(oldMasterIp, newMasterIp);
            Files.write(Paths.get(newConfigurationDir, "core-site.xml"), coreSiteXml.getBytes(StandardCharsets.UTF_8));
            log.info("core-site.xml updated with Master IP: " + newMasterIp);
        }
    }

    private String appendToClassPathString(String oldGlobalClasspath, String newConfPath) {
        StringBuilder sb = new StringBuilder(oldGlobalClasspath);
        int ndx = oldGlobalClasspath.lastIndexOf(',');
        if (!newConfPath.endsWith("/")) newConfPath += "/";
        if (ndx < 0) {
            sb.insert(0, newConfPath + ",");
        } else {
            sb.insert(ndx, "," + newConfPath);
        }
        return sb.toString();
    }
}
