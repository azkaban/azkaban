#Azkaban Containerized Executions - High Level Design Doc
**Authors:** @Arvind Pruthi, @Janki Akhani, @Shardool Shardool, @Deepak Jaiswal, @Aditya Sharma, @Abhishek Nath

## Background/Overview of Bare-Metal Architecture
![](figures/azkaban-architecture.png)

- Each Azkaban Cluster is made of a single web server, one or more “bare metal” executor servers and a mysql db
to track history and state.
- Each bare metal server is capable of orchestrating a few 10s of flows simultaneously depending on the resource
requirements of each flow.
- The executor server polls flows that are ready to run on a frequent basis from MySQL based queue (“dispatch
logic”) (Current config: 1/s).
### Azkaban Executor Server Responsibilities
#### Dispatch
- Executor Server polls the MySQL based queue for flows once/s contingent on resource availability.
- Sets up the flow (Parses all properties, de-serializes the workflow to build an in-memory graph, downloads
binaries in Project Cache if missing, allocates resources: thread pool, execution directory etc.) and finally:
- Kicks off the orchestration of the flow.

#### Orchestration
- Executor Server manages a thread pool per flow, which allows multiple jobs to run in parallel. 
- Hadoop tokens are fetched from the Hadoop name node to allow launching of a YARN application.
- Each job is launched in it’s own separate process with the flow admin account.
- During the orchestration process, the Executor server manages the state machine of the flow, keeps the database
up to date with flow/job state and finally flushes flow logs to the database.

#### Flow Management
- Executor server is an end-point for AJAX APIs to respond to requests such as Pause, Kill, Resume etc. Flows are
killed when they reach the SLA limit of 10 days.

#### Log Management
- Executor server’s AJAX API endpoint supports streaming logs for live flows/jobs. When a flow/job finishes, the
completed logs are pushed to the Azkaban db in 15MB chunks (Configurable). 

#### Deployment
- During deployment, new binaries are updated on the bare-metal server and some tests are performed to verify the
sanity of the machine.
- If tests pass, the Executor Server is put in inactive mode, upon which it stops polling for flows except for
those pinned to that particular executor. 
- A new Executor Server is launched with new binaries and is marked as active; thereby, resuming normal function.

### Issues with Bare Metal Executor Server Model
#### Noisy-neighbor Issues (No resource Isolation)
Azkaban’s Job Type model allows users to inject arbitrary code on the executor. It is easy to write a jobtype
that consumes an unfair share of resources (Compute, Memory or Storage) that overwhelmes the executor server;
thereby causing noisy-neighbor issues.

#### Scaling/Maintenance Issues (Inflexible Infrastructure)
Scaling is a step function requiring servers to be provisioned by SREs. This is rooted in a decade old
architecture. The current Azkaban architecture does not benefit from the advanacements made in flexible
provisioning, found commonplace in cloud.

#### No Canary for Azkaban binaries or the jobtypes
Azkaban executors are the gateway to critical compute infrastructure. It brings together layers of platform
binaries such as for Hadoop, security manager, spark etc besides Azkaban’s own binaries, configurations,
jobtypes. Currently, there is no canary in place for a fine-grained tune-up. Experience within Linkedin shows
how painful it can be to roll out major upgrades without a proper canary system.

#### Mismatched Queueing from YARN
Azkaban implements it’s own queueing to maximize the use of bare-metal Executors. This queueing is often
mismatched from YARN queues causing issues downstream.

#### Deployment Issues
Update to Job Type code requires a jobtype plugin deployment to happen on all Executors. 
As part of deployment, the inactive ExecutorServer process may continue for up to 10 days to satisfy Azkaban’s
SLAs, occupying a lot of memory and CPU. Hence, if a deployment has issues, successive attempts to rectify the
problem leave additional beefy ExecutorProcesses running, causing memory sap leading to GC pauses and OOMs.
They also pollute metrics.

## Key Requirements for Containerization
1. Azkaban Web Server should be able to Launch flows in independent containers, thereby giving a fully
isolated environment for each flow.

2. Be able to respond quickly in response to Spikes in demand (Flexible Infrastructure).

3. Allow the components that make up Azkaban (Platform/Client binaries for Hadoop, Hive etc.), Azkaban itself,
jobtypes to evolve independently of each other.

   a. Give the evolution control for Platform, Azkaban and Jobtypes to their corresponding owners.
   
   b. Provide users a way to override default binary versions of Azkaban/jobtypes etc. to the version of
      their choice (Helpful during development process of infrastructure -- Azkaban/jobtypes/platform).
    
   c. Provide plumbing for a fine-grained Canary system that can allow Azkaban/jobtypes and platform full
      control of ramping up their binaries, independent of each other.
   

### Future Extensions
1. Provide the fine-grained Canary system for Multiple components that make up Azkaban to help in their
independent evolution.

## High Level Design
![](figures/containerized-high-level-arch.png)

### Design Summary 
1. Azkaban will follow a **Disposable Container** model. This implies that whenever a flow is to be launched,
the **dispatch logic** will launch a fresh Pod and the pod is destroyed at the conclusion of the flow.

2. Isolation is achieved per flow (Not jobs). Jobs/subflows that are a part of a flow, will be run within the
confines of the pod launched for the purpose of the flow.

3. The pod will be launched with default compute/memory resources, but override parameters will be available
to request more resources for the flow orchestration.

4. For this design iteration, the web server will stay outside of k8s. This does not preclude the need for
the web server to talk to flow pods to fetch logs or send control commands (Such as Cancel). Thus, in order
to talk to the flow pods, an Envoy Proxy based Ingress Controller is introduced, which will allow the web
server to communicate with Flow Pods. There is no need to set node ports for flow pods.

5. In order to satisfy [key Requirement #3](#Key-Requirements-for-Containerization), the execution
environment for flow pods will be constructed dynamically at run-time each time a pod is launched.

   a. Azkaban will provide a mechanism to dynamically select versions of components that constitute
      a functional Azkaban Executor environment at dispatch time.
      
   b. Following this, a series of init containers will pull various components to compose the complete
execution environment.

   c. The dynamic selection process will ultimately make way to provide canary capability for various
   Azkaban components.
   
   d. The design also introduces a few Admin APIs to make the task of image management easier.

### Image Management

### Container Image & Job Types

1. To achieve [key Requirement #4](#Key-Requirements), the final container image that actually runs a given
flow is built dynamically within init-containers when the flow pod is launched. The required layers will be
discovered as laid out in the [dispatch logic](#dispatch-logic).

2. Each layer defines a separate component that is allowed to evolve independently. Each of these layers
will be extremely simple as each layer is concerned about downloading a single image and adding it to the
right location. A separate init container will be launched for each such image.

3. Here is an example image definition for KPJ (Kafka Push Job):
``` 
  FROM container-image-registry.corp.linkedin.com/lps-image/linkedin/rhel7-base-image/rhel7-base-image:0.16.9

  ARG KPJ_URL=https://artifactory.corp.linkedin.com:8083/artifactory/DDS/com/linkedin/kafka-push-job/kafka-push-job/0.2.61/kafka-push-job-0.2.61.jar
  
  RUN curl $KPJ_URL --output ~/kafka-push-job-0.2.61.jar
```

4. At dispatch time, a graph walk will be performed to find out all the job types that
the flow intends to execute. Their "default" version will be picked from the database
table. Users can override the default version through runtime properties. The version
maps to the specific Image. Details are described in the [Dispatch Logic Section](#dispatch-logic).

### Dispatch Logic 
 
1. Whenever a flow is ready to run (By schedule, by data triggers or manually through an API call),
the AZ Web Server will “dispatch” it in a separate pod in a k8s namespace dedicated for Azkaban. The pod
will be disposable, meaning it will be destroyed once the flow is finished. This is return to push-based
dispatch, which is more suitable for containerization user case.

2. There will be only 1 flow container inside a pod. The life of the pod is limited to the lifetime of the flow.
The pod will be destroyed once the flow finishes execution. This is return to the push-based dispatch logic,
which is more suitable for this use case.

3. In order to dispatch the flow in a new pod, a YAML file will be constructed dynamically for the pod
definition. This will include:

   a) A list of Images/Versions to be put together to compose a functional environment. This includes Platform
   images for things like: Hive, Hdfs etc., Azkaban binaries, and finally a list of images corresponding
   to each jobtype that will be invoked as part of the flow and their corresponding images.
   More details in [Image Management section](#image-management) 

   b) Azkaban Configuration to be used for the executing container.

   c) Pointer to the Executable Flow that the Pod is supposed to run.

4. Kubernetes secret will be used to package:

   a) Credentials to access mysql database for flow/job status updates.

   b) Azkaban Executor Server Certificate that will be used to fetch Hadoop Tokens before launching jobs on Yarn.

   c) Azkaban Executor Kafka Event Certificate (Different cert) with ACLs to send events to the Kafka topic.

   d) Azkaban Executor Kafka Logging Certificate with ACLs to dispatch logs from the running container to Kafka.

### Init Containers 
[Init containers](https://kubernetes.io/docs/concepts/workloads/pods/init-containers/) is a Kubernetes concept.
The role of init containers is to put together everything necessary to launch a fully functional flow container.

![](figures/init-container-images.png)

1. Kubernetes will run the init containers in a sequence before the control is given to the application container
as shown in the picture.

2. Each Jobtype that is included in the flow will correspond to an init container that gets
initiated. This init conatainer will take the layer for the jobtype binary and add it to the volume
for the application container.

### Flow Container

1. A new class: "FlowContainer" will be created by refactoring code from the FlowRunnerManager. The FlowContainer
class is basically a highly simplified version of FlowRunnerManager. The assumption is that this class will
handle a single flow, so a number of things can be simplified. Things like: Polling logic to fetch flows or
logic around status tracking of multiple flows can be completely cut out. Also no cleanup of execution directory
or cache is needed as the pod will be destroyed after the flow finishes. This will have the side effect of cutting
down the tech debt.

2. The web server needs to talk to the Kubernetes pods as the executor server hosts an AJAX API
endpoint for various control operations such as Cancel, Pause, Resume, FetchLogs etc. For the web server
to continue using this API endpoint, we need to enable communication between the Webserver (Which is outside
the k8s cluster) and the flow container pods. For this reason, we plan to use the 
[Ambassador Ingress Controller](https://www.getambassador.io/docs/latest/topics/running/ingress-controller/)
between the Web Server and the Flow Container Pods. More regarding the ingress controller [here](#ingress-controller).

3. In the long-run, we do plan to bring in web server into Kubernetes as well, thereby eliminating the
Ingress Controller. For the short-term, we will continue to live with the added complexity.

4. At Linkedin our internal analysis shows that APIs beyond Cancel, FetchLogs and Ping are rarely used. For
the sake of simplicity, we are also contemplating how to eliminate the API endpoint on flow container completely.

5. For now, the Flow/Log Mgmt AJAX endpoints will continue to be supported. But we plan to disable
all APIs other than: Cancel, FetchLog, FlowStatus & Ping (Full list of APIs). This will help us
keep the possibility of eliminating rest of the APIs alive in the medium/long term.

6. During flow execution, flow and job life cycle events may need to be sent to Kafka as well as job/flow
status updates may need to be made in Mysql db. For sending events to Kafka, azkaban-exec-server’s cert
issued by a valid certificate authority will be used to authenticate flow containers. This and MySQL
credentials will be pulled from Kubernetes secret.

### Ingress Controller

1. As mentioned in the [Flow Container Section](#flow-container), we will be utilizing the
[Ambassador Ingress Controller](https://www.getambassador.io/docs/latest/topics/running/ingress-controller/) as
a reverse proxy.

2. The ingress controller will provide necessary routing between web server and the flow pods running on
kubernetes infrastructure. A key aspect of this architecture is that the routes between web server and flow pods
need to be updated dynamically at flow dispatch time and right after a flow finishes. The Ambassador Ingress
Controller enables this by providing APIs that are key to dynamically updating these routes. This is realized
through [annotations](https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/).

### Logging in Executor
The AJAX API endpoint (FetchLog) will continue to work as the means for the Azkaban UI to pull logs for the
flows/jobs in progress. When a flow finishes, the logs will be copied to a well-defined directory structure
in HDFS. This is different & better from today. Currently, the logs are split in chunks and copied to the
Mysql db, which is a serious anti-pattern.

### Image Management
The Azkaban Deployable docker image will be constructed from 4 different layers that come together during
build time in: Azkaban-docker MP.

The final image creates a version set of layers that always rolls forward (Explained later).

The layered approach lends flexibility and minimizes rebuilding of docker images.
Azkaban Layered Docker Image Creation (Details doc - [4])
Hadoop-Base is the bottom layer that provides a stable Hadoop/Hive/Spark image, provided by Grid SRE.
Azkaban-Platform adds a layer of Platform binaries ( PIG, Ksudo, etc.).
Azkaban-Base adds core Azkaban binaries & Azkaban owned job types. The “.tar.gz” files pulled for the purpose
are built and pushed to the artifactory by Azkaban-Distribution MP. Azkaban-Deployable adds binaries for
Jobtype Plugins that Azkaban supports. Jobtype owners are responsible (like today) for publishing their
“tar.gz” files and point to the latest in “az-cluster-config” MP. These binaries will be pulled at build time
from the artifactory and made part of this docker image.

Dynamically Updating Azkaban Config
Current World: A key issue with Azkaban today is that config changes require a deployment cycle. This is because
the configs are bundled as part of the Azkaban Deployable RPM. This obviously impacts siteup.


In the Kubernetes world, we plan to make configs load dynamically. The Flow Container Pods will be launched
with an “emptyDir” mount. Using the Init container or an “ENTRYPOINT” bash script, the git repo for Azkaban
Cluster Config (Already on a separate git repository) will be downloaded and the configs will be consumed
from here.

Deployable Image Versioning & Version Sets
Each layer within the docker image will follow a 3-integer versioning (<MAJOR>.<MINOR>.<PATCH>). A
“roll-forward” strategy will be used for any change to the image. This implies that if the version of the
image in a certain layer (/docker file) is changed, all the image versions in the next layers (/docker files)
will be rolled forward. Hence, the upper most layer: Azkaban-Deployable will always have a version bump.
Example: If Azkaban-Base needs to be rolled back to v0.1.1 from 0.1.2. Hence “Azkaban-JobTypes” docker file
will be built and published. Resulting versions are:

Before
After
Azkaban-Platform -> v0.0.1
Azkaban-Base -> v0.1.2
Azkaban-Deployable -> v0.2.3
Azkaban-Platform -> v0.0.1
Azkaban-Base -> v0.1.1
Azkaban-Deployable -> v0.2.4
Version Set for Azkaban-Deployable
A version set is the metadata added to the Azkaban-Deployable image as a label. This version set is supposed to contain all the versioning information of the different images/binaries. Here is a sample version set: 

Azkaban-Deployable v0.5.8
{
 hadoop-base: 0.0.22
 azkaban-platform: 0.0.1
 azkaban-base: 0.1.2
 az-cluster-config: 0.1.122
 azkaban-distribution-mp: 0.0.199
 kafka-push-job: 0.0.453
 pinot-build-and-push-job: 0.0.123
 wormhole-push-job: 0.0.156
 pig: 0.0.15
 ksudo: 0.0.145
}

Docker Image Rollout
Azkaban-Deployable docker image will be made available in the image repository published as part of the CRT Build and Release of the new Azkaban-Docker MP.
A MySQL Table will track the meta information of each released docker image. Schema will include a Enum based stability tag: “TEST”, “UNSUITABLE”, “STABLE”, “LTS”. Exact criteria/schema will be defined in [4].
A canary system to be designed on top of containerization will leverage the image tracking table for rollouts.
Dev Testing (Azkaban-Deployable-Test image)
In order to make the dev testing process more efficient, an additional docker file: “azkaban-deployable-test” will be built on top of “azkaban-deployable” & pushed to the registry. This is to cut overheads in dev testing.
The “azkaban-deployable-test” image will add: “overlay-dev-binaries.sh” script as a docker ENTRYPOINT. 
This script will look for dev-provided Azkaban binaries on the developer’s home dir on HDFS and copy these on top of stock binaries in the image, before starting the Flow Container.
This automation should save significant dev time by eliminating the need to rebuild docker images and to upload GBs of images for testing. The jar copy process is something that developers are familiar with.
Rollout of JobTypes
After testing their changes to the JobType, the JobType owner will publish the “.tar.gz” file containing the binaries to the artifactory.
They will specify the new version in the Az-Cluster-Config MP and raise an RB requesting for review from the Azkaban team.
Post review, RB will be pushed by the JobType owners.
Subsequent builds on the Azkaban-Docker MP will build the image with the updated changes to the Azkaban JobType.
Deployment Automation (CICD) 
Current World (No Containerization):
Code Changes → Dev Testing → Release Tagging → Testing on Pokemon → Testing on Yugioh  → Final deployment during deployment cycle
Each push to os-azkaban MP results in the build deployed from Azkaban Distribution MP on Pokemon.
When deployment is to be started, a tag from Pokemon (usually the latest), is promoted to Yugioh.
Yugioh is then used as base deployment and all the other Azkaban clusters are deployed by promoting from Yugioh. The deployment happens in this order,
Dropbox, GPU clusters (eg, mlearn-alpha), Faro, Holdems, and War.
Enough soak time is provided to make sure the release is stable. Overall, it takes about two weeks to complete the deployment process.
Containerized World:
Code Changes → Dev Testing → Azkaban-deployable image build → Daily Jenkins job deploys latest image on EI setup & runs integration tests → Deploys latest image on PROD setup & runs integration tests → Stability of release candidate is recorded in MySQL db → Final deployment based on a stable image
Actual deployment cycle will follow the same sequence as today. However, the deployment time should be considerably shorter.
Integration Tests will constitute a suite of flows that test Azkaban as the integration point for Grid Services. Will leverage synergy with the Groundhog day team to come up with Integration test flows.
To aid development testing, existing tests will be enhanced to create a smoke test suite. Detailed TBD.

Non-Containerized World
Containerized World



## How does the proposal solve Issues with Bare Metal Model?
1. Full Resource Isolation - 1 DAG per container.
2. Allows linear scaling both up and down based on demand.
3. Deployments need not impact running containers. Ramp-up for new binaries can be developed in a fine-grained way; no step function involved.
4. Once Azkaban/job binaries make it to HDFS, they don’t need to make a second round.

**Bonus benefits...**
1. A lot of Executor Server related tech-debt disappears: in-memory state in executor servers, onsite overhead in managing server health, executor deployment issues etc.
2. Deployment of ExecutorServer becomes straightforward: Push new docker image to the image-registry
3. Deployment takes more than a week on bare metal, it could be much less with containerization as executor servers take most of the time in deployment.
4. Flow executions can be made resumable-on-crash.



