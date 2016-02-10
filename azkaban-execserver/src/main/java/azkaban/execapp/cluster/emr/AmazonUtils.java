package azkaban.execapp.cluster.emr;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by christopher.wu on 6/24/15.
 *
 * Contains methods for interacting with the Amazon SDKs.
 */
public class AmazonUtils {



    private AWSCredentials credentials = null;
    private AmazonCloudWatchClient cloudWatchClient = null;
    private AmazonS3Client s3Client = null;
    private AmazonElasticMapReduceClient emrClient = null;

    private Logger log;

    public AmazonUtils() {
        this(null, null);
    }
    public AmazonUtils(AWSCredentials credentials, Logger logger) {
        if (credentials != null) {
            this.credentials = credentials;
        } else {
            this.initialize(); // get credentials
        }
        createClients();
        log = logger != null ? logger : Logger.getLogger(AmazonUtils.class);
    }

    public void initialize() {
        DefaultAWSCredentialsProviderChain providerChain = new DefaultAWSCredentialsProviderChain();
        this.credentials = providerChain.getCredentials();
        createClients();
    }


    /**
     * Load credentials from hadoop conf / s3 keys
     */
    protected static Configuration loadHadoopConfig(String configPath) throws MalformedURLException {
        Configuration hadoopConfig = new Configuration();
        ArrayList resources = new ArrayList();
        resources.add((new File(configPath)).toURI().toURL());
        URLClassLoader ucl = new URLClassLoader((URL[]) resources.toArray(new URL[resources.size()]));
        hadoopConfig.setClassLoader(ucl);
        return hadoopConfig;
    }

    private void createClients() {
        cloudWatchClient = new AmazonCloudWatchClient(credentials).withRegion(Regions.US_WEST_2);
        emrClient = new AmazonElasticMapReduceClient(credentials).withRegion(Regions.US_WEST_2);
        s3Client = new AmazonS3Client(credentials);
    }



    public AWSCredentials getCredentials() {
        return this.credentials;
    }

    public AWSCredentials initializeFromHadoopConf(String configurationPath) throws MalformedURLException {
        if (credentials != null) return credentials;

        DefaultAWSCredentialsProviderChain providerChain = new DefaultAWSCredentialsProviderChain();
        try {
            credentials = providerChain.getCredentials();
        } catch (AmazonClientException e) {
            if (configurationPath != null && !configurationPath.isEmpty()) {
                Configuration hadoopConfig = loadHadoopConfig(configurationPath);
                String accessKey = hadoopConfig.get("fs.s3.awsAccessKeyId");
                String secretKey = hadoopConfig.get("fs.s3.awsSecretAccessKey");
                credentials = new BasicAWSCredentials(accessKey, secretKey);
            } else {
                throw new AmazonClientException(e.getMessage() + ". Also tried (and failed) to find credentials from hadoop configuration.");
            }
        }
        createClients();
        return credentials;
    }

    public void terminateCluster(String clusterId) {
        emrClient.terminateJobFlows(new TerminateJobFlowsRequest(Collections.singletonList(clusterId)));
    }

    public List<JSONObject> getClusterInfos() throws JSONException {
        ListClustersResult result;
        try {
             result = emrClient.listClusters(new ListClustersRequest().withClusterStates("STARTING",
                    "BOOTSTRAPPING", "WAITING", "RUNNING", "TERMINATING"));
        } catch (AmazonServiceException disconnected) {
            // It's possible we get disconnected after a while.
            this.initialize();
            result = emrClient.listClusters(new ListClustersRequest().withClusterStates("STARTING",
                    "BOOTSTRAPPING", "WAITING", "RUNNING", "TERMINATING"));
        }

        List<JSONObject> infos = new ArrayList<>(result.getClusters().size());

        for (ClusterSummary summary : result.getClusters()) {
            JSONObject info = new JSONObject();
            info.put("name", summary.getName());
            info.put("id", summary.getId());
            info.put("status", summary.getStatus().getState());
            info.put("creationTime", summary.getStatus().getTimeline().getCreationDateTime());

            Cluster cluster = emrClient.describeCluster(new DescribeClusterRequest().withClusterId(summary.getId())).getCluster();
            long persistent = cluster.getTags().stream().filter(t -> t.getKey().equals("persistent") && t.getValue().equals("true")).count();
            info.put("persistent", persistent > 0);

            ListInstancesRequest req = new ListInstancesRequest().withClusterId(summary.getId())
                    .withInstanceGroupTypes(InstanceGroupType.MASTER);

            info.put("ip", "");
            ListInstancesResult res = emrClient.listInstances(req);
            if (res.getInstances().size() > 0 && res.getInstances().get(0).getPrivateIpAddress() != null) {
                info.put("ip", res.getInstances().get(0).getPrivateIpAddress());
            }

            info.put("masterNodes", "");
            info.put("coreNodes", "");
            info.put("taskNodes", "");
            for (InstanceGroup group :  emrClient.listInstanceGroups(new ListInstanceGroupsRequest().withClusterId(summary.getId())).getInstanceGroups()) {
                String desc = group.getName() + ": " + group.getRunningInstanceCount() + " " + group.getInstanceType() + "<br>";
                if (group.getInstanceGroupType().equals("MASTER")) {
                    info.put("masterNodes", info.get("masterNodes") + desc);
                }
                else if (group.getInstanceGroupType().equals("CORE")) {
                    info.put("coreNodes", info.get("coreNodes") + desc);
                }
                else {
                    info.put("taskNodes", info.get("taskNodes") + desc);
                }
            }

            infos.add(info);
        }

        return infos;
    }






    public static void main(String[] args) throws Exception {
//        //TODO: put this in a proper test
//        Props jobProps = new Props();
//        Props sysProps = new Props();
////        jobProps.put(EmrClusterController.EMR_CLUSTER_ID_PROP, "j-N2KV79UPTX11");
//        sysProps.put("hadoop.conf.dir", "/azkaban-solo/conf");
//        jobProps.put("working.dir", "/Users/jsoumet/projects/docker-azkaban-solo/executions/68");
//        jobProps.put(EmrClusterController.AZKABAN_EXEC_ID_PROP, "90");
//        jobProps.put(EmrClusterController.AZKABAN_FLOW_ID_PROP, "closestConnection");
//        jobProps.put(EmrCluster.EMR_CONF_CLUSTER_SECURITYGROUP, "sg-d02b03b5");
//        jobProps.put(EmrCluster.EMR_CONF_CLUSTER_SUBNET, "subnet-99ea65fc");
//        jobProps.put(EmrCluster.EMR_CONF_BOOTSTRAP_ACTIONS, "Customize for RelateIQ,s3://usw2-relateiq-emr-scripts/scripts/emr-customize-relateiq-4.sh");
//
//        AWSCredentials creds = new AmazonUtils().initializeFromHadoopConf("/azkaban-solo/conf");
//        EmrClusterController cluster = new EmrClusterController(creds, null);
//        ///, , "/azkaban-solo/conf/", "/Users/jsoumet/projects/docker-azkaban-solo/executions/68/conf/"
//        Props combinedProps = new Props(jobProps, sysProps);
//        cluster.initialize(combinedProps, "/azkaban-solo/conf/");
//        cluster.blockUntilClusterIsReady();
////        List<Datapoint> datapoints = new AmazonUtils(null).getClusterMetric("j-1NOC2YM8O2AME", "HDFSUtilization", "Percent");
////
////        Comparator<Datapoint> comp = (d1, d2) -> Double.compare(d1.getAverage(), d2.getAverage());
////
////        Datapoint max = datapoints.stream().max(comp).get();
////        System.out.println(max.toString());
    }









    /*

    public BootstrapActionConfig createBootstrapAction(String name, String path, List<String> args) {
        ScriptBootstrapActionConfig script = new ScriptBootstrapActionConfig(path, args);
        BootstrapActionConfig bootstrap = new BootstrapActionConfig(name, script);

        return bootstrap;
    }

    public String runEmrJobFlow(Props props) {
        StepConfig setupTmpFolder = new StepConfig("Change hadoop /tmp folder permissions",
                new HadoopJarStepConfig("s3://usw2-relateiq-emr-scripts/jars/hadoop-common-2.4.0.jar")
                        .withMainClass("org.apache.hadoop.fs.FsShell").withArgs("-chmod", "777", "/tmp"));

        StepConfig setupPig = new StepConfig("Setup pig",
                new HadoopJarStepConfig("s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar")
                        .withArgs("s3://us-west-2.elasticmapreduce/libs/pig/pig-script", "--base-path",
                                "s3://us-west-2.elasticmapreduce/libs/pig/", "--install-pig", "--pig-versions", "0.12.0"));

        StepConfig setupHadoopDebugging = new StepConfig("Setup hadoop debugging",
                new HadoopJarStepConfig("s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar")
                        .withArgs("s3://us-west-2.elasticmapreduce/libs/state-pusher/0.1/fetch"));

        return this.runEmrJobFlow(props, Arrays.asList(setupTmpFolder, setupPig, setupHadoopDebugging));
    }

    public String runEmrJobFlow(Props props, Collection<StepConfig> steps) {
        String clusterName = "Azkaban-" + props.getString("azkaban.flow.flowid", "unknown_flowid")
                + "-" + props.getString("azkaban.flow.execid", "unknown_execid");

        ListClustersResult res = client.listClusters(new ListClustersRequest().withClusterStates("WAITING"));
        for (ClusterSummary cluster : res.getClusters()) {
            if (clusterName.equals(cluster.getName())) {
                return cluster.getId();
            }
        }

        JobFlowInstancesConfig instancesConfig = this.createEMRClusterInstanceConfig(props);

        RunJobFlowRequest request = new RunJobFlowRequest(clusterName, instancesConfig);
        request.setLogUri(EMR_LOG_PATH);
        request.setSteps(steps);
        request.setAmiVersion("3.9.0");
        request.setServiceRole("EMR_DefaultRole");
        request.setJobFlowRole("EMR_EC2_DefaultRole");
        request.setVisibleToAllUsers(true);

        if (props.containsKey("emr.cluster.tags")) {
            request.setTags(Arrays.stream(props.getString("emr.cluster.tags").split(";"))
                    .map(p -> { String[] p2 = p.split(","); return new Tag(p2[0], p2[1]); }).collect(Collectors.toList()));
        }

        if (props.containsKey("emr.cluster.bootstrap")) {
            request.setBootstrapActions(Arrays.stream(props.getString("emr.cluster.bootstrap").split(";"))
                    .map(p -> {
                        List<String> p2 = Arrays.asList(p.split(","));
                        return createBootstrapAction(p2.get(0), p2.get(1), p2.subList(Math.min(2, p2.size()), p2.size()));
                    }).collect(Collectors.toList()));
        }

        RunJobFlowResult result = client.runJobFlow(request);

        try {
            DescribeClusterRequest descRequest = new DescribeClusterRequest().withClusterId(result.getJobFlowId());
            ListInstancesRequest listRequest = new ListInstancesRequest().withClusterId(result.getJobFlowId());

            int maxWaitMinutes = 30;
            while (maxWaitMinutes-- > 0) {
                DescribeClusterResult descResult = client.describeCluster(descRequest);
                if (descResult.getCluster().getStatus().getState().contains("TERMINAT")) {
                    throw new RuntimeException("Cluster is terminated");
                }

                if (descResult.getCluster().getStatus().getState().equals("WAITING") &&
                    client.listInstances(listRequest).getInstances().stream().allMatch(inst -> inst.getStatus().getState().equals("RUNNING"))) {
                    return result.getJobFlowId();
                }

                this.log.info("Waiting for cluster to spin up...");
                Thread.sleep(60000);
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        throw new RuntimeException("Waited for 30 minutes but not cluster is not ready. Check Amazon console");
    }

    public String getMasterIP(String clusterId) {
        ListInstancesRequest req = new ListInstancesRequest().withClusterId(clusterId)
                .withInstanceGroupTypes(InstanceGroupType.MASTER);

        ListInstancesResult res = client.listInstances(req);

        if (res.getInstances().size() > 0) {
            return res.getInstances().get(0).getPrivateIpAddress();
        }

        return "";
    }

    public void maybeTerminateCluster(String clusterId, Props sysProps) throws Exception {
        TerminateJobFlowsRequest req = new TerminateJobFlowsRequest(Collections.singletonList(clusterId));
        client.terminateJobFlows(req);

        this.sendClusterNotificationEmail(sysProps, Arrays.asList("chris.wu@relateiq.com", "sre@relateiq.com"),
                "EMR Cluster " + clusterId + " is terminating", "See the Azkaban EMR tab for more details.");
    }

    public void maybeTerminateCluster(Props jobProps, Props sysProps) throws Exception {
        String clusterName = "Azkaban-" + jobProps.getString("azkaban.flow.flowid", "unknown_flowid")
                + "-" + jobProps.getString("azkaban.flow.execid", "unknown_execid");

        ListClustersResult result = client.listClusters(new ListClustersRequest().withClusterStates("STARTING",
                "BOOTSTRAPPING", "WAITING"));
        for (ClusterSummary cluster : result.getClusters()) {
            if (clusterName.equals(cluster.getName())) {
                TerminateJobFlowsRequest req = new TerminateJobFlowsRequest(Collections.singletonList(cluster.getId()));
                client.terminateJobFlows(req);

                this.sendClusterNotificationEmail(sysProps, Arrays.asList("chris.wu@relateiq.com", "sre@relateiq.com"),
                        "EMR Cluster " + cluster.getId() + " is terminating", "See the Azkaban EMR tab for more details.");
                break;
            }
        }
    }







    public void resizeCluster(String clusterId, int numTaskNodes) {
        client.listInstanceGroups(new ListInstanceGroupsRequest().withClusterId(clusterId)).getInstanceGroups().stream()
                .filter(group -> group.getInstanceGroupType().equals("TASK"))
                .forEach(group -> {
                    client.modifyInstanceGroups(new ModifyInstanceGroupsRequest().withInstanceGroups(new
                            InstanceGroupModifyConfig(group.getId(), numTaskNodes)));
                });
    }



    private void sendClusterNotificationEmail(Props sysProps, Collection<String> recipients, String subject, String body) {
        try {
            if (sysProps.containsKey("hadoop.conf.dir")) {
                File azkabanPropsFile = new File(sysProps.get("hadoop.conf.dir"), "azkaban.properties");
                Props azkabanProps = new Props(null, azkabanPropsFile);

                String mailHost = azkabanProps.getString("mail.host", "localhost");
                String mailUser = azkabanProps.getString("mail.user", "");
                String mailPassword = azkabanProps.getString("mail.password", "");
                String mailSender = azkabanProps.getString("mail.sender", "");

                int mailTimeout = azkabanProps.getInt("mail.timeout.millis", 10000);
                EmailMessage.setTimeout(mailTimeout);
                int connectionTimeout = azkabanProps.getInt("mail.connection.timeout.millis", 10000);
                EmailMessage.setConnectionTimeout(connectionTimeout);

                EmailMessage message = new EmailMessage(mailHost, mailUser, mailPassword);
                message.setFromAddress(mailSender);
                message.addAllToAddress(recipients);
                message.setSubject(subject);
                message.println(body);

                message.sendEmail();
            }
        } catch (MessagingException | IOException e) {
            e.printStackTrace();
        }
    }

    private void runCommand(String cmd) {
        try {
            Process e = Runtime.getRuntime().exec(cmd);
            BufferedReader stdError = new BufferedReader(new InputStreamReader(e.getErrorStream()));

            String s;
            while ((s = stdError.readLine()) != null) {
                System.out.println(s);
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to run command " + cmd, e);
        }
    }

    public List<Datapoint> getClusterMetric(String clusterId, String metricName, String metricUnit, Date startTime) {
        GetMetricStatisticsRequest request = new GetMetricStatisticsRequest().withNamespace("AWS/ElasticMapReduce")
                .withStartTime(startTime).withEndTime(new Date()).withPeriod(300)
                .withStatistics("Average").withUnit(metricUnit).withMetricName(metricName)
                .withDimensions(new Dimension().withName("JobFlowId").withValue(clusterId));

        GetMetricStatisticsResult result = cloudWatchClient.getMetricStatistics(request);
        return result.getDatapoints();
    }

/* */



}
