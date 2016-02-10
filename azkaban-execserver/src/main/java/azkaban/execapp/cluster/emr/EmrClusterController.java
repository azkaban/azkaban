//package azkaban.execapp.cluster.emr;
//
//import azkaban.utils.Props;
//import com.amazonaws.auth.AWSCredentials;
//import com.amazonaws.regions.Regions;
//import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
//import com.amazonaws.services.elasticmapreduce.model.*;
//import com.amazonaws.services.s3.AmazonS3Client;
//import org.apache.commons.io.FileUtils;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.log4j.Logger;
//
//import java.io.File;
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.util.*;
//import java.util.stream.Collectors;
//
///**
// * Created by jsoumet on 10/13/15.
// */
//public class EmrClusterController {
//
//    Logger log;
//
//    public static final Set<ClusterState> RUNNING_STATES = new HashSet<>(Arrays.asList(
//            new ClusterState[]{
//                    ClusterState.STARTING, ClusterState.BOOTSTRAPPING, ClusterState.RUNNING, ClusterState.WAITING
//            }
//    ));
//    public static final Set<ClusterState> READY_STATES = new HashSet<>(Arrays.asList(
//            new ClusterState[]{
//                    ClusterState.RUNNING, ClusterState.WAITING
//            }
//    ));
//
//    public static final String EMR_NOOP = "emr.noop";
//    public static final String EMR_CLUSTER_ID_PROP = "emr.cluster.id";
//    public static final String EMR_CLUSTER_DEFAULT_PROP = "emr.cluster.default";
//
//    public static final String EMR_CONF_CLUSTER_TERMINATE_ON_ERROR = "emr.cluster.terminate.error";
//    private static final boolean EMR_DEFAULT_CLUSTER_TERMINATE_ON_ERROR = true;
//
//    public static final String EMR_CONF_CLUSTER_TERMINATE_ON_COMPLETION = "emr.cluster.terminate.completion";
//    private static final boolean EMR_DEFAULT_CLUSTER_TERMINATE_ON_COMPLETION = false;
//
//    public static final String EMR_CLUSTER_NEW_PROP = "emr.cluster.new";
//
//
//    public static final String AZKABAN_FLOW_ID_PROP = "azkaban.flow.flowid";
//    public static final String AZKABAN_EXEC_ID_PROP = "azkaban.flow.execid";
//
//
//
//    public static final int MAX_CLUSTER_LOOKUP_RETRIES = 15; // attempts to find cluster by name 6 times then fail
//
//    public static final int CLUSTER_READINESS_TIMEOUT = 30; // 30 minutes
//
//    private AmazonElasticMapReduceClient client;
//    private AmazonS3Client s3Client;
//
//    private String clusterId = null;
//    private String clusterName = null;
//    private String newConfigurationDir = null;
//    private String newClassPath = null;
//
//
//    protected EmrClusterController(AWSCredentials credentials, Logger logger) {
//        log = logger != null ? logger : Logger.getLogger(AmazonUtils.class);
//        client = new AmazonElasticMapReduceClient(credentials).withRegion(Regions.US_WEST_2);
//        s3Client = new AmazonS3Client(credentials);
//    }
//
//
//    private String computeClusterNameFromProps(Props jobProps) {
//        return "Azkaban - Transient Cluster"
//                + " - [" + jobProps.getString(AZKABAN_FLOW_ID_PROP, "unknown_flowid")
//                + ":" + jobProps.getString(AZKABAN_EXEC_ID_PROP, "unknown_execid")
//                + "]";
//    }
//
//
//    private Cluster findClusterById(String clusterId) {
//        DescribeClusterRequest descRequest = new DescribeClusterRequest().withClusterId(clusterId);
//        try {
//            DescribeClusterResult clusterDescription = client.describeCluster(descRequest);
//            return clusterDescription.getCluster();
//        } catch (InvalidRequestException e) {
//            throw new RuntimeException("Error looking for cluster " + clusterId, e);
//        }
//    }
//
//    private ClusterSummary findClusterByName(String clusterName) {
//        return findClusterByName(clusterName, null, null);
//    }
//
//    private ClusterSummary findClusterByName(String clusterName, Collection<String> clusterStates) {
//        return findClusterByName(clusterName, clusterStates, null);
//    }
//
//    private ClusterSummary findClusterByName(String clusterName, Set<ClusterState> clusterStates) {
//        return findClusterByName(clusterName, clusterStates.stream().map(ClusterState::toString).collect(Collectors.toList()));
//    }
//
//    private ClusterSummary findClusterByName(String clusterName, Collection<String> clusterStates, String marker) {
//        ListClustersRequest req = new ListClustersRequest();
//        if (marker != null) req.setMarker(marker);
//        if (clusterStates != null) req.setClusterStates(clusterStates);
//
//        ListClustersResult res = client.listClusters(req);
//        for (ClusterSummary cluster : res.getClusters()) {
//            if (clusterName.equals(cluster.getName())) {
//                return cluster;
//            }
//        }
//        if (res.getClusters().size() == 50) { // more results
//            return findClusterByName(clusterName, clusterStates, res.getMarker());
//        }
//        return null;
//    }
//
//    private Cluster describeCluster(String clusterId) {
//        return client.describeCluster(new DescribeClusterRequest().withClusterId(clusterId)).getCluster();
//    }
//
//
//    /**
//     * Configure with specific cluster Id
//     */
//    private void configureByClusterId(String clusterId) {
//        log.info("Cluster id set to " + clusterId);
//        this.clusterId = clusterId;
//        if (!isClusterActive()) {
//            throw new RuntimeException("Cluster is not active. ClusterId = " + clusterId);
//        }
//    }
//
//
//
//    /**
//     * This function needs to create the cluster and block until the master becomes available so we can get
//     */
//    private void createNewCluster(Props jobProps, String clusterName) throws IOException {
//        log.info("Creating new cluster with name: " + clusterName);
//        ClusterSummary findCluster = findClusterByName(clusterName, RUNNING_STATES);
//        if (findCluster != null) {
//            clusterId = findCluster.getId();
//        } else {
//            clusterId = EmrCluster.createNewCluster(jobProps, clusterName, client, s3Client);
//        }
//
//    }
//
//    /**
//     * Set clusterId based on tag
//     */
//    private void configureFromClusterName(String clusterName) throws InterruptedException {
//        int retries = MAX_CLUSTER_LOOKUP_RETRIES;
//        while (--retries > 0) {
//            log.info("Looking for an eligible cluster on EMR...");
//            ClusterSummary cluster = findClusterByName(clusterName, RUNNING_STATES);
//            if (cluster != null) {
//                this.clusterId = cluster.getId();
//                Cluster clusterInfo = describeCluster(cluster.getId());
//                log.info("Found cluster " + cluster.getId() + " with name " + cluster.getName());
//                log.info("Cluster state: " + cluster.getStatus().getState() + ".");
//                log.info("Cluster created on: " + cluster.getStatus().getTimeline().getCreationDateTime() + ".");
//                clusterInfo.getTags().forEach(t -> log.info(t.getKey() + "=" + t.getValue()));
//                return;
//            } else {
//                Thread.sleep(60000); // sleep 60 seconds
//            }
//        }
//        throw new RuntimeException("Could not find a cluster for this job. Was searching for name " + clusterName);
//    }
//
//    public void blockUntilClusterIsReady() throws InterruptedException, IOException {
//        int waitMinutes = CLUSTER_READINESS_TIMEOUT;
//        while (--waitMinutes > 0) {
//            if (isClusterReady()) {
//                // cluster is ready.. update config files if necessary
//                log.info("Cluster is ready!");
//                updateConfigFilesWithMasterIp();
//                return;
//            }
//            log.info("Waiting for cluster " + clusterId + " to be ready...");
//            Thread.sleep(60000);
//        }
//        throw new RuntimeException("Failed to connect to cluster " + clusterId + " after " + CLUSTER_READINESS_TIMEOUT + " minutes.");
//    }
//
//    public String getNewHadoopConfigurationDir() {
//        if (newConfigurationDir == null) throw new RuntimeException("Tried to get hadoop conf dir but cluster was not initialized.");
//        return newConfigurationDir;
//    }
//    public String getNewClassPath() {
//        if (newClassPath == null) throw new RuntimeException("Tried to get class path but cluster was not initialized.");
//        return newClassPath;
//    }
//
//
//    public void initialize(Props configProps, String oldGlobalClasspath) throws IOException, InterruptedException {
//        String originalConfigurationDir = configProps.getString("hadoop.conf.dir");
//        newConfigurationDir = configProps.getString("working.dir") + "/conf";
//        newClassPath = appendToClassPathString(oldGlobalClasspath, newConfigurationDir);
//        log.info("Emr configuration initialization");
//        // Cluster naming convention
//        clusterName = computeClusterNameFromProps(configProps);
//
//        if ((new File(newConfigurationDir)).exists()) { // another job in the flow already set-up the configuration
//            log.info("Configuration folder already initialized by another job. Will skip EMR cluster management.");
//            return ;
//        }
//
//        synchronized (EmrClusterController.class) { // lock to prevent race condition
//            if (configProps.containsKey(EMR_CLUSTER_NEW_PROP) && configProps.getBoolean(EMR_CLUSTER_NEW_PROP, false)) {
//                log.info("Creating a new EMR cluster...");
//                createNewCluster(configProps, clusterName);
//                createConfigFiles(originalConfigurationDir);
//                return;
//            }
//
//            if (configProps.containsKey(EMR_CLUSTER_ID_PROP)) {
//                String clusterId = configProps.getString(EMR_CLUSTER_ID_PROP);
//                log.info("Specific EMR cluster requested: " + clusterId);
//                configureByClusterId(clusterId);
//                if (isClusterActive()) {
//                    log.info("Found cluster " + clusterId);
//                } else {
//                    throw new RuntimeException("Could not find cluster " + clusterId);
//                }
//                createConfigFiles(originalConfigurationDir);
//                return ;
//            }
//
//            if (configProps.containsKey(EMR_CLUSTER_DEFAULT_PROP)) {
//                this.log.info("Will run on default persistent EMR cluster");
//                //TODO: find clusterId for current long-running cluster.
//                //TODO: this could mean we dont have to restart the azk docker when using a new cluster
//                //TODO: but need to think about this feature first
//                //TODO: for now just return the old conf path
//                //createConfigFiles(originalConfigurationDir, newConfigurationDir, getMasterIP());
//                //return newClassPath;
//                newClassPath = oldGlobalClasspath;
//                return;
//            }
//        }
//        return;
//    }
//
//
//    public String getMasterIP() {
//        if (clusterId != null) {
//            ListInstancesRequest req = new ListInstancesRequest().withClusterId(clusterId)
//                    .withInstanceGroupTypes(InstanceGroupType.MASTER);
//            ListInstancesResult res = client.listInstances(req);
//            if (res.getInstances().size() > 0) {
//                return res.getInstances().get(0).getPrivateIpAddress();
//            }
//        } else {
//            throw new RuntimeException("Called getMasterIP but did not provide clusterId");
//        }
//        throw new RuntimeException("Called getMasterIP but could not find that cluster. ClusterId=" + clusterId);
//    }
//
//    public void terminateCluster() {
//        if (clusterId != null) {
//            log.info("Terminating cluster " + clusterId);
//            client.terminateJobFlows(new TerminateJobFlowsRequest(Collections.singletonList(clusterId)));
//        } else {
//            throw new RuntimeException("Attempted to terminate cluster before initializing.");
//        }
//    }
//
//
//    public void maybeTerminateCluster(Props jobProps, boolean jobErrored) {
//        if (jobErrored && jobProps.getBoolean(EMR_CONF_CLUSTER_TERMINATE_ON_ERROR, EMR_DEFAULT_CLUSTER_TERMINATE_ON_ERROR)) {
//            log.info("Terminating cluster due to job errors.");
//            terminateCluster();
//        } else if (jobProps.getBoolean(EMR_CONF_CLUSTER_TERMINATE_ON_COMPLETION, EMR_DEFAULT_CLUSTER_TERMINATE_ON_COMPLETION)) {
//            terminateCluster();
//        } else {
//            log.info("Not terminating cluster");
//        }
//    }
//
//    /**
//     * Call EMR api to find out if cluster is in running state
//     */
//    public boolean isClusterActive() {
//        if (clusterId != null) {
//            Cluster cluster = findClusterById(this.clusterId);
//            if (RUNNING_STATES.contains(ClusterState.fromValue(cluster.getStatus().getState()))) {
//                return true;
//            }
//        } else {
//            throw new RuntimeException("Called isClusterActive but clusterId was null.");
//        }
//        return false;
//    }
//
//    public boolean isClusterReady() {
//        synchronized (EmrClusterController.class) {
//            try {
//                // sleep for random amount of time. max 5 seconds. To avoid hitting the EMR api too frequently
//                Thread.sleep((long)(Math.random() * 5000));
//            } catch (InterruptedException e) {
//                // ignore
//            }
//
//            if (clusterId == null) try {
//                configureFromClusterName(clusterName);
//            } catch (InterruptedException e) {
//                throw new RuntimeException("Could not find cluster.");
//            }
//            if (clusterId != null) {
//                Cluster cluster = findClusterById(this.clusterId);
//                if (READY_STATES.contains(ClusterState.fromValue(cluster.getStatus().getState()))) {
//                    return true;
//                } else if (!RUNNING_STATES.contains(ClusterState.fromValue(cluster.getStatus().getState()))) {
//                    throw new RuntimeException("Called isClusterReady but cluster was terminated.");
//                }
//            } else {
//                throw new RuntimeException("Called isClusterReady but clusterId was null.");
//            }
//            return false;
//        }
//    }
//
//
//    /**
//     * Create executions/xx/conf folder so that Yarn can pick up the new cluster
//     * Synchronized so 2 instances dont try to create the folder at the same time
//     */
//    private void createConfigFiles(String oldConfPath) {
//        try {
//            log.info("Creating config files in " + newConfigurationDir);
//            FileUtils.copyDirectory(new File(oldConfPath), new File(newConfigurationDir));
//        } catch (IOException e) {
//            throw new RuntimeException("Failed to create configuration folder.", e);
//        }
//    }
//
//    /**
//     * Called when cluster is available. will modify once
//     */
//    private void updateConfigFilesWithMasterIp() throws IOException {
//        // replace old ip with new one
//        Configuration configuration = AmazonUtils.loadHadoopConfig(newConfigurationDir);
//        String oldMasterIp = configuration.get("hadoop.master.ip");
//        String newMasterIp = getMasterIP(); // from EMR
//
//        if (!oldMasterIp.equals(newMasterIp)) { // maybe we already changed it
//            String coreSiteXml = new String(Files.readAllBytes(Paths.get(newConfigurationDir, "core-site.xml")), StandardCharsets.UTF_8);
//            coreSiteXml = coreSiteXml.replaceAll(oldMasterIp, newMasterIp);
//            Files.write(Paths.get(newConfigurationDir, "core-site.xml"), coreSiteXml.getBytes(StandardCharsets.UTF_8));
//            log.info("core-site.xml updated with Master IP: " + newMasterIp);
//        }
//    }
//
//    private String appendToClassPathString(String oldGlobalClasspath, String newConfPath) {
//        StringBuilder sb = new StringBuilder(oldGlobalClasspath);
//        int ndx = oldGlobalClasspath.lastIndexOf(',');
//        if (!newConfPath.endsWith("/")) newConfPath += "/";
//        if (ndx < 0) {
//            sb.insert(0, newConfPath + ",");
//        } else {
//            sb.insert(ndx, "," + newConfPath);
//        }
//        return sb.toString();
//    }
//
//
//}
