package azkaban.flowtrigger.security;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_NATIVE_LIB_FOLDER;
import static azkaban.flowtrigger.security.SecurityPropName.AZKABAN_KEYTAB_LOCATION;
import static azkaban.flowtrigger.security.SecurityPropName.AZKABAN_PRINCIPAL;
import static azkaban.flowtrigger.security.SecurityPropName.HADOOP_SECURITY_MANAGER_CLASS_PARAM;
import static azkaban.flowtrigger.security.SecurityPropName.OBTAIN_BINARY_TOKEN;

import azkaban.Constants;
import azkaban.Constants.JobProperties;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.Props;

public class AddHardCodedProps {

  public static void thatEnableTokenPrefetch(Props props) {
    // boolean properties
    // ./plugins/jobtypes/commonprivate.properties:append.submit.user=false
    props.put(HadoopSecurityManager.APPEND_SUBMIT_USER, "true");

    props.put(HadoopSecurityManager.OBTAIN_JOBTRACKER_TOKEN, "true");
    props.put(HadoopSecurityManager.OBTAIN_NAMENODE_TOKEN, "true");
    props.put(SecurityPropName.OBTAIN_JOBHISTORYSERVER_TOKEN, "true");

    // ./plugins/jobtypes/commonprivate.properties:obtain.binary.token=true
    props.put(OBTAIN_BINARY_TOKEN, "true");
    // ./plugins/jobtypes/hadoopJava/private.properties:azkaban.should.proxy=true
    //./plugins/jobtypes/reportalteradata/plugin.properties:azkaban.should.proxy=false
    props.put(HadoopSecurityManager.ENABLE_PROXYING, "true");

    // Job property
    // ./deploy/PinotBuildAndPushJob/PinotBuildAndPushJob-0.0.385/private.properties
    //./deploy/PinotBuildAndPushJob/PinotBuildAndPushJob/private.properties
    //./executions/23582991/common.properties
    //./executions/23583254/common.properties
    //./executions/23582494/grid-common.properties
    props.put(JobProperties.ENABLE_JOB_SSL, "true");


    // Why is obtain.hcat.token in jobtype plugins and executions ?
    // ./plugins/jobtypes/hadoopJava/private.properties
    // ./executions/23575703/common.properties
    // ./executions/23581385/war_common.properties
    props.put(HadoopSecurityManager.OBTAIN_HCAT_TOKEN, "true");


    // String properties
    props.put(JobProperties.USER_TO_PROXY, "azktest");
    // "hadoop.security.manager.class"
    // ./plugins/jobtypes/commonprivate.properties:hadoop.security.manager.class=azkaban.security.HadoopSecurityManager_H_2_0
    props.put(HADOOP_SECURITY_MANAGER_CLASS_PARAM, "azkaban.security.HadoopSecurityManager_H_2_0");

    // ./plugins/jobtypes/commonprivate.properties:azkaban.native.lib=/export/apps/azkaban/native
    props.put(AZKABAN_SERVER_NATIVE_LIB_FOLDER, "/export/apps/azkaban/native");

    // ./plugins/jobtypes/common.properties:hadoop.home=/export/apps/hadoop/latest
    // ./executions/23575703/common.properties
    props.put("hadoop.home", "/export/apps/hadoop/latest");

    // ??
    // ./conf/azkaban.properties:hadoop.conf.dir.path=/export/apps/hadoop/site/etc/hadoop
    props.put("hadoop.conf.dir", "");

    // ./plugins/jobtypes/commonprivate.properties:proxy.keytab.location=/export/apps/hadoop/keytabs/azkaban.service.keytab
    // ./plugins/jobtypes/commonprivate.properties:proxy.user=azkaban/lva1-warazexec07.grid.linkedin.com@GRID.LINKEDIN.COM
    // ./plugins/jobtypes/commonprivate.properties:azkaban.security.credential=com.linkedin.azkaban.security.DataVaultIdentityProvider
    props.put(AZKABAN_KEYTAB_LOCATION, "/export/apps/hadoop/keytabs/azkaban.service.keytab");
    // For War cluster
    props.put(AZKABAN_PRINCIPAL, "azkaban/lva1-warazexec07.grid.linkedin.com@GRID.LINKEDIN.COM");
    // For Holdem cluster
    props.put(AZKABAN_PRINCIPAL, "azkaban/ltx1-holdemaz04.grid.linkedin.com@GRID.LINKEDIN.COM");

    props.put(Constants.ConfigurationKeys.CUSTOM_CREDENTIAL_NAME, "com.linkedin.azkaban.security.DataVaultIdentityProvider");

    // ./plugins/jobtypes/commonprivate.properties:submit.user.suffix=@GRID.LINKEDIN.COM
    // ./executions/23551858/peoplematch-properties-file_props_7767984632253761928_tmp:azkaban.flow.submituser=jmhe
    props.put(HadoopSecurityManager.SUBMIT_USER_SUFFIX, "@GRID.LINKEDIN.COM");
    props.put(Constants.FlowProperties.AZKABAN_FLOW_SUBMIT_USER, "azktest");


    // ./plugins/jobtypes/commonprivate.properties:azkaban.csr.endpoint.uri=https://1.grestin.prod-ltx1.atd.prod.linkedin.com:10180/grestin/request_service_certificate?fabric=prod-lva1&application={hadoop-headless-user}&instance=war01&tag=azkaban-exec-server
    // Used by az-grestin-client
    /*
    props.put("azkaban.csr.endpoint.uri",
        "https://1.grestin.prod-ltx1.atd.prod.linkedin"
            + ".com:10180/grestin/request_service_certificate?fabric=prod-lva1&application={hadoop-headless-user}&instance=war01&tag=azkaban-exec-server");
     */

    // Holdem04
    props.put("azkaban.csr.endpoint.uri",
        "https://1.grestin.ei-ltx1.atd.stg.linkedin"
            + ".com:10180/grestin/request_service_certificate?fabric=ei-ltx1&application={hadoop"
            + "-headless-user}&instance=holdemaz04&tag=azkaban-exec-server");


    /*
     * VALUES FOR KEYS LISTED IN
     * az-grestin-client/src/main/java/com/linkedin/azkaban/security/CertConstants.java
     */
    // ./conf/azkaban.properties:
    // azkaban.csr.truststore.location=/export/content/security/services/azkaban-exec-server/EI_cacerts;

    // ./plugins/jobtypes/commonprivate.properties:azkaban.csr.truststore.location=/export/content/security/services/azkaban-exec-server/EI_cacerts
    // Needed ny Data vault/ Grestin
    props.put("azkaban.csr.truststore.location", "/export/content/security/services/azkaban-exec-server/EI_cacerts");
    props.put("azkaban.csr.keystore.location", "/export/content/security/services/azkaban-exec-server/azkaban-exec.p12");

    // StringList properties
    // EXTRA_HCAT_LOCATION
    // ./executions/23574944/node-properties.flow:
    //     other_hcat_location: thrift://ltx1-holdemhcat01.grid.linkedin.com:7552
    // ./executions/23574944/join-edges-node-properties_props_275227555535475485_tmp:
    //     other_hcat_location=thrift\://ltx1-holdemhcat01.grid.linkedin.com\:7552
    // ./executions/23579030/grids_war_common.properties:
    //     other_hcat_location=thrift://lva1-warhcat01.grid.linkedin.com:7552


    // StringListFromCluster properties
    // EXTRA_HCAT_CLUSTERS


    /////////
    ///////// fs.getDelegationToken(p.getString(JobProperties.USER_TO_PROXY));
    /////////
  }
}
