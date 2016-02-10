package azkaban.execapp.cluster.emr;

import azkaban.utils.Props;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.json.Jackson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Created by jsoumet on 10/17/15.
 */
public class EmrCluster {

    private static final List<String> allowedInstanceTypes = Arrays.asList("m3.xlarge", "m3.2xlarge", "c3.xlarge", "c3.2xlarge",
            "c3.4xlarge", "c3.8xlarge", "g2.2xlarge", "r3.xlarge", "r3.2xlarge", "r3.4xlarge", "r3.8xlarge", "i2.xlarge",
            "i2.2xlarge", "i2.4xlarge", "i2.8xlarge");

    public static final String EMR_CONF_CLUSTER_INSTANCE_TASK_TYPE = "emr.cluster.instance.task.type";
    private static final String EMR_DEFAULT_CLUSTER_INSTANCE_TASK_TYPE = "r3.xlarge";
    public static final String EMR_CONF_CLUSTER_INSTANCE_TASK_COUNT = "emr.cluster.instance.task.count";
    private static final int EMR_DEFAULT_CLUSTER_INSTANCE_TASK_COUNT = 1;
    public static final String EMR_CONF_CLUSTER_INSTANCE_TASK_REQUEST_SPOT = "emr.cluster.instance.task.spot";
    private static final boolean EMR_DEFAULT_CLUSTER_INSTANCE_TASK_REQUEST_SPOT = false; //TODO review
    public static final String EMR_CONF_CLUSTER_INSTANCE_TASK_SPOT_PRICE = "emr.cluster.instance.task.spot.price";
    public static final String EMR_CONF_CLUSTER_INSTANCE_CORE_TYPE = "emr.cluster.instance.core.type";
    private static final String EMR_DEFAULT_CLUSTER_INSTANCE_CORE_TYPE = "i2.xlarge";
    public static final String EMR_CONF_CLUSTER_INSTANCE_CORE_COUNT = "emr.cluster.instance.core.count";
    private static final int EMR_DEFAULT_CLUSTER_INSTANCE_CORE_COUNT = 1;
    public static final String EMR_CONF_CLUSTER_INSTANCE_MASTER_TYPE = "emr.cluster.instance.master.type";
    private static final String EMR_DEFAULT_CLUSTER_INSTANCE_MASTER_TYPE = "m3.xlarge";

    public static final String EMR_CONF_CLUSTER_TAGS = "emr.cluster.tags";
    public static final String EMR_CONF_BOOTSTRAP_ACTIONS = "emr.cluster.bootstrap";

    public static final String EMR_CONF_CLUSTER_SUBNET = "emr.cluster.subnet";
    public static final String EMR_CONF_CLUSTER_SECURITYGROUP = "emr.cluster.securitygroup";


    public static final String EMR_CONF_EC2_SSH_KEY_NAME = "emr.ec2.key";
    private static final String EMR_DEFAULT_EC2_SSH_KEY_NAME = "SparkEMR";

    public static final String EMR_CONF_LOG_PATH = "emr.log.path";
    private static final String EMR_DEFAULT_LOG_PATH = "s3://usw2-hadoop-logs/logs/";

    // 3.x
    public static final String EMR_CONF_AMI_VERSION = "emr.ami.version";

    // 4.1.0
    public static final String EMR_CONF_RELEASE_LABEL = "emr.release.label";
    private static final String EMR_DEFAULT_RELEASE_LABEL = "emr-4.1.0";
    public static final String EMR_CONF_APPLICATIONS = "emr.applications";
    private static final String EMR_DEFAULT_APPLICATONS = "Hadoop;Pig;Spark";

    public static final String EMR_CONF_JOB_FLOW_ROLE = "emr.jobflow.role";
    public static final String EMR_CONF_SERVICE_ROLE = "emr.service.role";

    public static final String EMR_CONF_APP_CONFIGURATION_S3_PATH = "emr.app.configuration.s3.path";
    private static final String EMR_DEFAULT_APP_CONFIGURATION_S3_PATH = "s3://usw2-relateiq-emr-scripts/scripts/hadoo-emr-settings.json";


    private static final int MAX_CORE_INSTANCES = 15;
    private static final int MAX_TASK_INSTANCES = 150;

    private static final String EC2_SPOT_PRICE_HISTORY_URL = "http://spot-price.s3.amazonaws.com/spot.js";


    private static boolean validateInstanceTypes(String... instanceTypes) {
        for (String instanceType: instanceTypes) {
            if (!allowedInstanceTypes.contains(instanceType)) {
                return false;
            }
        }
        return true;
    }

    private static JobFlowInstancesConfig createEMRClusterInstanceConfig(Props props) {
        String taskInstanceType = props.getString(EMR_CONF_CLUSTER_INSTANCE_TASK_TYPE, EMR_DEFAULT_CLUSTER_INSTANCE_TASK_TYPE);
        int taskInstanceCount = props.getInt(EMR_CONF_CLUSTER_INSTANCE_TASK_COUNT, EMR_DEFAULT_CLUSTER_INSTANCE_TASK_COUNT);
        String coreInstanceType = props.getString(EMR_CONF_CLUSTER_INSTANCE_CORE_TYPE, EMR_DEFAULT_CLUSTER_INSTANCE_CORE_TYPE);
        int coreInstanceCount = props.getInt(EMR_CONF_CLUSTER_INSTANCE_CORE_COUNT, EMR_DEFAULT_CLUSTER_INSTANCE_CORE_COUNT);
        String masterInstanceType = props.getString(EMR_CONF_CLUSTER_INSTANCE_MASTER_TYPE, EMR_DEFAULT_CLUSTER_INSTANCE_MASTER_TYPE);

        if (!validateInstanceTypes(taskInstanceType, coreInstanceType, masterInstanceType)) {
            throw new RuntimeException("An invalid instance type(s) was selected: master=" + masterInstanceType
                    + " core=" + coreInstanceType + " task=" + taskInstanceType);
        }

        List<InstanceGroupConfig> instanceGroups = new ArrayList<>();
        InstanceGroupConfig masterInstanceGroupConfig =
                new InstanceGroupConfig(InstanceRoleType.MASTER.toString(), masterInstanceType, 1).withName("Master node");
        instanceGroups.add(masterInstanceGroupConfig);

        if (coreInstanceCount > 0 && coreInstanceCount <= MAX_CORE_INSTANCES) {
            InstanceGroupConfig coreInstanceGroupConfig =
                    new InstanceGroupConfig(InstanceRoleType.CORE.toString(), coreInstanceType, coreInstanceCount).withName("Core nodes");
            instanceGroups.add(coreInstanceGroupConfig);
        } else {
            throw new RuntimeException("Core instance count must be between 1 and " + MAX_CORE_INSTANCES);
        }


        if (taskInstanceCount > 0 && taskInstanceCount <= MAX_TASK_INSTANCES) {
            InstanceGroupConfig taskInstanceGroupConfig =
                    new InstanceGroupConfig(InstanceRoleType.TASK.toString(), taskInstanceType, taskInstanceCount).withName("Task nodes");
            if (props.getBoolean(EMR_CONF_CLUSTER_INSTANCE_TASK_REQUEST_SPOT, EMR_DEFAULT_CLUSTER_INSTANCE_TASK_REQUEST_SPOT)) {
                taskInstanceGroupConfig.setMarket(MarketType.SPOT);
                taskInstanceGroupConfig.setBidPrice(props.getString(EMR_CONF_CLUSTER_INSTANCE_TASK_SPOT_PRICE,
                        String.format("%.3f", getBidPrice(taskInstanceType) * 1.1)));
            }
            instanceGroups.add(taskInstanceGroupConfig);
        } else {
            throw new RuntimeException("Task instance count must be between 1 and " + MAX_TASK_INSTANCES);
        }

        String securityGroup = props.get(EMR_CONF_CLUSTER_SECURITYGROUP);
        if (securityGroup == null) throw new RuntimeException("EMR security group not specified");
        String ec2SubnetId = props.get(EMR_CONF_CLUSTER_SUBNET);
        if (ec2SubnetId == null) throw new RuntimeException("EMR security group not specified");


        JobFlowInstancesConfig instancesConfig = new JobFlowInstancesConfig();
        instancesConfig.setEc2KeyName(props.getString(EMR_CONF_EC2_SSH_KEY_NAME, EMR_DEFAULT_EC2_SSH_KEY_NAME));
        instancesConfig.setInstanceGroups(instanceGroups);
        instancesConfig.setKeepJobFlowAliveWhenNoSteps(true);
        instancesConfig.setEc2SubnetId(ec2SubnetId);
        instancesConfig.setEmrManagedMasterSecurityGroup(securityGroup);
        instancesConfig.setEmrManagedSlaveSecurityGroup(securityGroup);
        return instancesConfig;
    }

    private static BootstrapActionConfig createBootstrapAction(String name, String path, List<String> args) {
        return new BootstrapActionConfig(name, new ScriptBootstrapActionConfig(path, args));
    }

    private static RunJobFlowRequest createRunJobFlowRequest(Props jobProps, String clusterName, JobFlowInstancesConfig instancesConfig, AmazonS3Client s3Client) throws IOException {
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
        if (jobProps.containsKey(EMR_CONF_RELEASE_LABEL) || ! jobProps.containsKey(EMR_CONF_AMI_VERSION)) { //default to new AMI
            request.setReleaseLabel(jobProps.getString(EMR_CONF_RELEASE_LABEL, EMR_DEFAULT_RELEASE_LABEL));
            request.setSteps(new ArrayList<StepConfig>() {{
                add(new StepConfig("Change hadoop /tmp folder permissions",
                        new HadoopJarStepConfig("s3://usw2-relateiq-emr-scripts/jars/hadoop-common-2.6.0.jar")
                                .withMainClass("org.apache.hadoop.fs.FsShell").withArgs("-chmod", "777", "/tmp")));
            }});
            request.setApplications(Arrays.stream(jobProps.getString(EMR_CONF_APPLICATIONS, EMR_DEFAULT_APPLICATONS).split(";"))
                    .map(a -> a.split(","))
                    .filter(r -> r.length >= 1)
                    .map(r -> new Application().withName(r[0]).withVersion(r.length == 2 ? r[1] : null))
                    .collect(Collectors.toList()));

            Logger log = Logger.getLogger(String.valueOf(EmrCluster.class));
            ProgressListener progress = event -> log.info(event.toString());
            request.setGeneralProgressListener(progress);
            request.setConfigurations(getClusterConfiguration(s3Client,
                    jobProps.getString(EMR_CONF_APP_CONFIGURATION_S3_PATH, EMR_DEFAULT_APP_CONFIGURATION_S3_PATH)));

            //TODO: find out wtf this does
            //            ScriptBootstrapActionConfig s3Config = new ScriptBootstrapActionConfig()
            //                    .withPath("file:///usr/bin/aws")
            //                    .withArgs("s3", "cp","s3://mybucket/myfolder/myobject","myFolder/");
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

    public static String createNewCluster(Props jobProps, String clusterName, AmazonElasticMapReduceClient client,
                                          AmazonS3Client s3Client) throws IOException {

        JobFlowInstancesConfig instancesConfig = createEMRClusterInstanceConfig(jobProps);
        RunJobFlowRequest request = createRunJobFlowRequest(jobProps, clusterName, instancesConfig, s3Client);
        RunJobFlowResult result = client.runJobFlow(request);
        return result.getJobFlowId();
    }











    /**
     * Get configuration from s3 url
     */
    public static List<Configuration> getClusterConfiguration(AmazonS3Client s3Client, String s3Url) throws IOException {
        AmazonS3URI appConfS3URL = new AmazonS3URI(s3Url);
        S3Object object = s3Client.getObject(new GetObjectRequest(appConfS3URL.getBucket(), appConfS3URL.getKey()));
        InputStream objectData = object.getObjectContent();
        Configuration[] conf = Jackson.getObjectMapper().readValue(objectData, Configuration[].class);
        return Arrays.asList(conf);
    }





    /**
     * Get spot instance pricing
     */
    public static double getBidPrice(String instanceType) {
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
                                        return osType.getAsJsonObject().getAsJsonObject("prices").get("USD").getAsDouble();
                                    }
                                }
                            }
                        }
                    }
                }
            }


            return 0;
        }
        catch (Exception e) {
            e.printStackTrace();
            return 0;
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




    public static void main(String[] args) {
        System.out.println(getBidPrice("m3.2xlarge"));
    }

}
