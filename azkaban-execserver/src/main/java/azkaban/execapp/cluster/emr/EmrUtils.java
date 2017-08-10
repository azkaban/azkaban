package azkaban.execapp.cluster.emr;

import azkaban.utils.Props;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Created by jsoumet on 2/8/16 for azkaban.
 */
public class EmrUtils {

    private transient static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(EmrUtils.class);

    private static final List<String> allowedInstanceTypes = Arrays.asList("m3.xlarge", "m3.2xlarge", "c3.xlarge", "c3.2xlarge",
            "c3.4xlarge", "c3.8xlarge", "g2.2xlarge", "r3.xlarge", "r3.2xlarge", "r3.4xlarge", "r3.8xlarge", "i2.xlarge",
            "i2.2xlarge", "i2.4xlarge", "i2.8xlarge");

    public static final String EMR_CONF_CLUSTER_INSTANCE_TASK_TYPE = "cluster.emr.instance.task.type";
    public static final String EMR_CONF_CLUSTER_INSTANCE_TASK_COUNT = "cluster.emr.instance.task.count";
    public static final String EMR_CONF_CLUSTER_INSTANCE_TASK_SPOT_PRICE = "cluster.emr.instance.task.spot.price";
    public static final String EMR_CONF_CLUSTER_INSTANCE_TASK_SPOT = "cluster.emr.instance.task.spot";

    public static final String EMR_CONF_CLUSTER_INSTANCE_CORE_TYPE = "cluster.emr.instance.core.type";
    public static final String EMR_CONF_CLUSTER_INSTANCE_CORE_COUNT = "cluster.emr.instance.core.count";
    public static final String EMR_CONF_CLUSTER_INSTANCE_CORE_SPOT_PRICE = "cluster.emr.instance.core.spot.price";
    public static final String EMR_CONF_CLUSTER_INSTANCE_CORE_SPOT = "cluster.emr.instance.core.spot";

    public static final String EMR_CONF_CLUSTER_INSTANCE_MASTER_TYPE = "cluster.emr.instance.master.type";
    public static final String EMR_CONF_CLUSTER_INSTANCE_MASTER_SPOT_PRICE = "cluster.emr.instance.master.spot.price";
    public static final String EMR_CONF_CLUSTER_INSTANCE_MASTER_SPOT = "cluster.emr.instance.master.spot";

    public static final String EMR_CONF_CLUSTER_TAGS = "cluster.emr.tags";
    public static final String EMR_CONF_BOOTSTRAP_ACTIONS = "cluster.emr.bootstrap";

    public static final String EMR_CONF_CLUSTER_SUBNET = "cluster.emr.subnet";
    public static final String EMR_CONF_CLUSTER_ZONETOSUBNET_PREFIX = "cluster.emr.zoneToSubnet.";
    public static final String EMR_CONF_CLUSTER_ZONE = "cluster.emr.zone";

    public static final String EMR_CONF_CLUSTER_SECURITYGROUP = "cluster.emr.securitygroup";

    // http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticmapreduce/model/JobFlowInstancesConfig.html#setServiceAccessSecurityGroup-java.lang.String-
    public static final String EMR_CONF_CLUSTER_SERVICE_ACCESS_SECURITYGROUP = "cluster.emr.service.access.securitygroup";

    public static final String EMR_CONF_EC2_SSH_KEY_NAME = "cluster.emr.ec2.key";

    public static final String EMR_CONF_LOG_PATH = "cluster.emr.log.path";
    private static final String EMR_DEFAULT_LOG_PATH = "s3://usw2-hadoop-logs/logs/";

    // 3.x
    public static final String EMR_CONF_AMI_VERSION = "cluster.emr.ami.version";

    // 4.1.0
    public static final String EMR_CONF_RELEASE_LABEL = "cluster.emr.release.label";
    private static final String EMR_DEFAULT_RELEASE_LABEL = "emr-4.1.0";
    public static final String EMR_CONF_APPLICATIONS = "cluster.emr.applications";
    private static final String EMR_DEFAULT_APPLICATONS = "Hadoop;Pig;Spark";

    public static final String EMR_CONF_JOB_FLOW_ROLE = "cluster.emr.jobflow.role";
    public static final String EMR_CONF_SERVICE_ROLE = "cluster.emr.service.role";


    private static final int MAX_CORE_INSTANCES = 15;
    private static final int MAX_TASK_INSTANCES = 150;

    private static final String EC2_SPOT_PRICE_HISTORY_URL = "http://spot-price.s3.amazonaws.com/spot.js";


    public static final Set<ClusterState> RUNNING_STATES = new HashSet<>(
            Arrays.asList(new ClusterState[]{ClusterState.STARTING, ClusterState.BOOTSTRAPPING, ClusterState.RUNNING, ClusterState.WAITING}));

    public static final Set<ClusterState> READY_STATES = new HashSet<>(Arrays.asList(new ClusterState[]{ClusterState.RUNNING, ClusterState.WAITING}));

    private enum EC2Zone {
        A, B, C;

        private static EC2Zone[] vals = values();

        public EC2Zone next() {
            return vals[(ordinal() + 1) % vals.length];
        }
    }

    private static final AtomicReference<EC2Zone> CURRENT_ZONE = new AtomicReference<>(EC2Zone.A);


    public static Cluster findClusterById(AmazonElasticMapReduceClient emrClient, String clusterId) {
        return emrClient.describeCluster(new DescribeClusterRequest().withClusterId(clusterId)).getCluster();
    }

    public static ClusterSummary findClusterByName(AmazonElasticMapReduceClient emrClient, String clusterName) {
        return findClusterByName(emrClient, clusterName, null, null);
    }

    public static ClusterSummary findClusterByName(AmazonElasticMapReduceClient emrClient, String clusterName, Collection<String> clusterStates) {
        return findClusterByName(emrClient, clusterName, clusterStates, null);
    }

    public static ClusterSummary findClusterByName(AmazonElasticMapReduceClient emrClient, String clusterName, Set<ClusterState> clusterStates) {
        return findClusterByName(emrClient, clusterName, clusterStates.stream().map(ClusterState::toString).collect(Collectors.toList()));
    }

    public static ClusterSummary findClusterByName(AmazonElasticMapReduceClient emrClient, String clusterName, Collection<String> clusterStates, String marker) {
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

    public static Optional<String> getMasterIP(AmazonElasticMapReduceClient emrClient, String clusterId) {
        if (clusterId != null) {
            ListInstancesRequest req = new ListInstancesRequest().withClusterId(clusterId).withInstanceGroupTypes(InstanceGroupType.MASTER);
            ListInstancesResult res = emrClient.listInstances(req);
            if (res.getInstances().size() > 0) {
                return Optional.of(res.getInstances().get(0).getPrivateIpAddress());
            }
        }
        return Optional.empty();
    }

    public static boolean terminateCluster(AmazonElasticMapReduceClient emrClient, String clusterId) {
        if (clusterId != null) {
            emrClient.terminateJobFlows(new TerminateJobFlowsRequest(Collections.singletonList(clusterId)));
            return true;
        }
        return false;
    }

    /**
     * Call EMR api to find out if cluster is in running state
     */
    public static boolean isClusterActive(AmazonElasticMapReduceClient emrClient, String clusterId) {
        if (clusterId != null) {
            Cluster cluster = findClusterById(emrClient, clusterId);
            return RUNNING_STATES.contains(ClusterState.fromValue(cluster.getStatus().getState()));
        }
        return false;
    }

    public static boolean isClusterReady(AmazonElasticMapReduceClient emrClient, String clusterId) {
        if (clusterId != null) {
            Cluster cluster = findClusterById(emrClient, clusterId);
            return READY_STATES.contains(ClusterState.fromValue(cluster.getStatus().getState()));
        }
        return false;
    }


    private static boolean validateInstanceTypes(String... instanceTypes) {
        for (String instanceType : instanceTypes) {
            if (!allowedInstanceTypes.contains(instanceType)) {
                return false;
            }
        }
        return true;
    }

    private static InstanceGroupConfig createInstanceGroupConfig(InstanceRoleType role, String instanceType,
                                                                 int count, boolean useSpot, Double spotPrice) throws InvalidEmrConfigurationException {
        if (!validateInstanceTypes(instanceType)) {
            throw new InvalidEmrConfigurationException("An invalid instance type(s) was selected: " + instanceType);
        }
        if (useSpot && spotPrice <= 0) {
            // (160% of default spot price)
            spotPrice = getBidPrice(instanceType).orElseThrow(() -> new InvalidEmrConfigurationException("Could not obtain spot price for " + instanceType)) * 1.6;
        }

        switch (role) {
            case MASTER:
                count = 1;
                break;
            case TASK:
                if (count <= 0 || count >= MAX_TASK_INSTANCES) {
                    throw new InvalidEmrConfigurationException("Invalid task instance count: " + count);
                }
                break;
            case CORE:
                if (count <= 0 || count >= MAX_CORE_INSTANCES) {
                    throw new InvalidEmrConfigurationException("Invalid core instance count: " + count);
                }
                break;
            default:
                throw new InvalidEmrConfigurationException("Invalid instance type");
        }
        String name = role.toString() + " nodes";
        InstanceGroupConfig instanceGroup = new InstanceGroupConfig(role, instanceType, count).withName(name);
        if (useSpot) {
            instanceGroup.setMarket(MarketType.SPOT);
            instanceGroup.setBidPrice(String.format("%.3f", spotPrice));
        }
        return instanceGroup;
    }

    public static JobFlowInstancesConfig createEMRClusterInstanceConfig(Props props) throws InvalidEmrConfigurationException {

        List<InstanceGroupConfig> instanceGroups = new ArrayList<>();
        instanceGroups.add(createInstanceGroupConfig(InstanceRoleType.MASTER,
                props.getString(EMR_CONF_CLUSTER_INSTANCE_MASTER_TYPE, "m3.xlarge"),
                1,
                props.getBoolean(EMR_CONF_CLUSTER_INSTANCE_MASTER_SPOT, true),
                props.getDouble(EMR_CONF_CLUSTER_INSTANCE_MASTER_SPOT_PRICE, 0)));
        instanceGroups.add(createInstanceGroupConfig(InstanceRoleType.CORE,
                props.getString(EMR_CONF_CLUSTER_INSTANCE_CORE_TYPE, "i2.xlarge"),
                props.getInt(EMR_CONF_CLUSTER_INSTANCE_CORE_COUNT, 1),
                props.getBoolean(EMR_CONF_CLUSTER_INSTANCE_CORE_SPOT, true),
                props.getDouble(EMR_CONF_CLUSTER_INSTANCE_CORE_SPOT_PRICE, 0)));

        instanceGroups.add(createInstanceGroupConfig(InstanceRoleType.TASK,
                props.getString(EMR_CONF_CLUSTER_INSTANCE_TASK_TYPE, "r3.xlarge"),
                props.getInt(EMR_CONF_CLUSTER_INSTANCE_TASK_COUNT, 1),
                props.getBoolean(EMR_CONF_CLUSTER_INSTANCE_TASK_SPOT, true),
                props.getDouble(EMR_CONF_CLUSTER_INSTANCE_TASK_SPOT_PRICE, 0)));


        String securityGroup = props.get(EMR_CONF_CLUSTER_SECURITYGROUP);
        if (securityGroup == null) throw new InvalidEmrConfigurationException("EMR security group not specified");

        String ec2SubnetId = getSubnetFromProps(props);

        JobFlowInstancesConfig instancesConfig = new JobFlowInstancesConfig();
        instancesConfig.setEc2KeyName(props.getString(EMR_CONF_EC2_SSH_KEY_NAME, "SparkEMR"));
        instancesConfig.setInstanceGroups(instanceGroups);
        instancesConfig.setKeepJobFlowAliveWhenNoSteps(true);
        instancesConfig.setEc2SubnetId(ec2SubnetId);
        instancesConfig.setEmrManagedMasterSecurityGroup(securityGroup);
        instancesConfig.setEmrManagedSlaveSecurityGroup(securityGroup);


        // To allow clusters in private subnets (https://jira.salesforceiq.com/browse/INFRA-7770)
        String serviceAccessSecurityGroup = props.get(EMR_CONF_CLUSTER_SERVICE_ACCESS_SECURITYGROUP);

        logger.info("** Azkaban Properties ** ");
        Set<String> keySet = props.getKeySet();
        for (String key : keySet) {
            String value = props.get(key);
            logger.info(key + ": " + value);
        }

        logger.info("Service Access Security Group: " + serviceAccessSecurityGroup);

        if (serviceAccessSecurityGroup != null && serviceAccessSecurityGroup.length() > 0 && !"n/a".equalsIgnoreCase(serviceAccessSecurityGroup)) {
            instancesConfig.setServiceAccessSecurityGroup(serviceAccessSecurityGroup);
        }

        return instancesConfig;
    }

    private static String getSubnetFromProps(Props props) throws InvalidEmrConfigurationException {
        Map<String, String> zoneToSubnet = props.getMapByPrefix(EMR_CONF_CLUSTER_ZONETOSUBNET_PREFIX);
        String zone = props.get(EMR_CONF_CLUSTER_ZONE);
        if (zone != null && zoneToSubnet.containsKey(zone.toUpperCase())) {
            return zoneToSubnet.get(zone.toUpperCase());
        }

        String ec2SubnetId = props.get(EMR_CONF_CLUSTER_SUBNET);
        if (ec2SubnetId != null) {
            return ec2SubnetId;
        }

        zone = CURRENT_ZONE.getAndUpdate(z -> z.next()).name();
        if (zoneToSubnet.containsKey(zone)) {
            return zoneToSubnet.get(zone);
        }

        throw new InvalidEmrConfigurationException("EMR subnet not specified");
    }

    private static BootstrapActionConfig createBootstrapAction(String name, String path, List<String> args) {
        return new BootstrapActionConfig(name, new ScriptBootstrapActionConfig(path, args));
    }

    public static RunJobFlowRequest createRunJobFlowRequest(Props jobProps, String clusterName, JobFlowInstancesConfig instancesConfig,
                                                            Collection<Configuration> clusterJSONConfiguration) {
        RunJobFlowRequest request = new RunJobFlowRequest(clusterName, instancesConfig);
        request.setLogUri(jobProps.getString(EMR_CONF_LOG_PATH, EMR_DEFAULT_LOG_PATH));
        request.setServiceRole(jobProps.getString(EMR_CONF_SERVICE_ROLE, "EMR_DefaultRole"));
        request.setJobFlowRole(jobProps.getString(EMR_CONF_JOB_FLOW_ROLE, "EMR_EC2_DefaultRole"));
        request.setVisibleToAllUsers(true);

        if (jobProps.containsKey(EMR_CONF_CLUSTER_TAGS)) {
            request.setTags(Arrays.stream(jobProps.getString(EMR_CONF_CLUSTER_TAGS).split(";"))
                    .map(p -> p.split(","))
                    .filter(r -> r.length == 2)
                    .map(r -> new Tag(r[0], r[1]))
                    .collect(Collectors.toList()));
        }

        // 4.1 vs AMI 3.8
        if (jobProps.containsKey(EMR_CONF_RELEASE_LABEL) || !jobProps.containsKey(EMR_CONF_AMI_VERSION)) { //default to new AMI
            request.setReleaseLabel(jobProps.getString(EMR_CONF_RELEASE_LABEL, EMR_DEFAULT_RELEASE_LABEL));
            /* Commenting this out because of issues on Lead Score Azkaban
            request.setSteps(new ArrayList<StepConfig>() {{
                add(new StepConfig("Change hadoop /tmp folder permissions",
                        new HadoopJarStepConfig("s3://usw2-relateiq-emr-scripts/jars/hadoop-common-2.6.0.jar")
                                .withMainClass("org.apache.hadoop.fs.FsShell").withArgs("-chmod", "777", "/tmp")));
            }});
            */
            request.setApplications(Arrays.stream(jobProps.getString(EMR_CONF_APPLICATIONS, EMR_DEFAULT_APPLICATONS).split(";"))
                    .map(a -> a.split(","))
                    .filter(r -> r.length >= 1)
                    .map(r -> new Application().withName(r[0]).withVersion(r.length == 2 ? r[1] : null))
                    .collect(Collectors.toList()));

            request.setConfigurations(clusterJSONConfiguration);

        } else { // requested AMI 3.8
            request.setAmiVersion(jobProps.getString(EMR_CONF_AMI_VERSION));
            request.setSteps(new ArrayList<StepConfig>() {{
                add(new StepConfig("Change hadoop /tmp folder permissions",
                        new HadoopJarStepConfig("s3://usw2-relateiq-emr-scripts/jars/hadoop-common-2.4.0.jar")
                                .withMainClass("org.apache.hadoop.fs.FsShell").withArgs("-chmod", "777", "/tmp")));
                add(new StepConfig("Setup pig",
                        new HadoopJarStepConfig("s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar")
                                .withArgs("s3://us-west-2.elasticmapreduce/libs/pig/pig-script", "--base-path",
                                        "s3://us-west-2.elasticmapreduce/libs/pig/", "--install-pig", "--pig-versions", "0.12.0")));
                add(new StepConfig("Setup hadoop debugging",
                        new HadoopJarStepConfig("s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar")
                                .withArgs("s3://us-west-2.elasticmapreduce/libs/state-pusher/0.1/fetch")));

            }});
        }

        if (jobProps.containsKey(EMR_CONF_BOOTSTRAP_ACTIONS)) {
            request.setBootstrapActions(Arrays.stream(jobProps.getString(EMR_CONF_BOOTSTRAP_ACTIONS).split(";"))
                    .map(p -> Arrays.asList(p.split(",")))
                    .filter(r -> r.size() >= 2)
                    .map(r -> createBootstrapAction(r.get(0), r.get(1), r.subList(Math.min(2, r.size()), r.size())))
                    .collect(Collectors.toList()));
        }
        return request;
    }

    /**
     * Get configuration from s3 url
     */
    public static List<Configuration> getClusterJSONConfiguration(AmazonS3Client s3Client, String s3Url) throws IOException {
        AmazonS3URI appConfS3URL = new AmazonS3URI(s3Url);
        S3Object object = s3Client.getObject(new GetObjectRequest(appConfS3URL.getBucket(), appConfS3URL.getKey()));
        InputStream objectData = object.getObjectContent();
        ObjectMapper objectMapper = new ObjectMapper();
        Configuration[] conf = objectMapper.readValue(objectData, Configuration[].class);
        return Arrays.asList(conf);
    }


    /**
     * Get spot instance pricing
     */
    public static Optional<Double> getBidPrice(String instanceType) {
        try {
            String sURL = EC2_SPOT_PRICE_HISTORY_URL;

            URL url = new URL(sURL);
            HttpURLConnection request = (HttpURLConnection) url.openConnection();
            request.connect();

            JsonParser jp = new JsonParser(); //from gson
            String content = readAll(new BufferedReader(new InputStreamReader((InputStream) request.getContent())));
            content = content.substring(content.indexOf("(") + 1, content.lastIndexOf(")")); // remote jsonP
            JsonElement root = jp.parse(content);

            JsonObject rootObj = root.getAsJsonObject();
            JsonArray regions = rootObj.getAsJsonObject("config").getAsJsonArray("regions");

            for (JsonElement region : regions) {
                if (region.getAsJsonObject().get("region").getAsString().equals("us-west-2")) {
                    for (JsonElement type : region.getAsJsonObject().getAsJsonArray("instanceTypes")) {
                        for (JsonElement size : type.getAsJsonObject().getAsJsonArray("sizes")) {
                            if (size.getAsJsonObject().get("size").getAsString().equals(instanceType)) {
                                for (JsonElement osType : size.getAsJsonObject().getAsJsonArray("valueColumns")) {
                                    if (osType.getAsJsonObject().get("name").getAsString().equals("linux")) {
                                        return Optional.of(osType.getAsJsonObject().getAsJsonObject("prices").get("USD").getAsDouble());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static String readAll(Reader rd) throws IOException {
        StringBuilder sb = new StringBuilder();
        int cp;
        while ((cp = rd.read()) != -1) {
            sb.append((char) cp);
        }
        return sb.toString();
    }


}
