package cloudflow.services;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import azkaban.user.User;
import cloudflow.daos.FlowDao;
import cloudflow.daos.FlowVersionDao;
import cloudflow.daos.ProjectDao;
import cloudflow.error.CloudFlowException;
import cloudflow.error.CloudFlowNotFoundException;
import cloudflow.error.CloudFlowValidationException;
import cloudflow.models.FlowResponse;
import cloudflow.models.FlowVersion;
import cloudflow.models.Project;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowServiceImpl implements FlowService {

  private static final String PROJECT_ID_PARAM = "project_id";
  private static final String PROJECT_VERSION_PARAM = "project_version";
  private static final String PROJECT_NAME_PARAM = "project_name";
  private static final String EXPAND_PARAM = "expand";
  private static final String VERSIONS_KEY = "versions";
  private static final Logger log = LoggerFactory.getLogger(ProjectServiceImpl.class);
  private final FlowDao flowDao;
  private final FlowVersionDao flowVersionDao;
  private final ProjectDao projectDao;

  @Inject
  public FlowServiceImpl(FlowDao flowDao, FlowVersionDao flowVersionDao, ProjectDao projectDao) {
    this.flowDao = flowDao;
    this.projectDao = projectDao;
    this.flowVersionDao = flowVersionDao;
  }


  @Override
  public FlowResponse getFlow(String flowId, Map<String, String[]> queryParamMap) {
    requireNonNull(flowId, "Flow id cannot be null");
    requireNonNull(queryParamMap, "query param map cannot be null");

    Optional<String> expandValue = Optional.empty();

    for (Map.Entry<String, String[]> entry : queryParamMap.entrySet()) {
      String key = entry.getKey();
      String[] value = entry.getValue();
      if (key.equals(EXPAND_PARAM)) {
        expandValue = Optional.of(extractArgumentValue(key, value));

        /*Currently only expand=versions is supported */
        if (!expandValue.get().equals(VERSIONS_KEY)) {
          throw new CloudFlowValidationException("Invalid value for expand: " + expandValue);
        }
      }
    }

    Optional<FlowResponse> flow = flowDao.getFlow(flowId);
    if (!flow.isPresent()) {
      String errorMsg = "Flow does not exist for id: " + flowId;
      log.error(errorMsg);
      throw new CloudFlowNotFoundException(errorMsg);
    }

    setFlowCreateAndLastModifiedInfo(flow.get());
    setLastFlowVersion(flow.get());

    /* If expand=versions is present in query params, get all the versions for the flow */
    if (expandValue.isPresent()) {
      flow.get().setFlowVersions(new ArrayList<>());
      flow.get().setFlowVersions(flowVersionDao.getAllVersions(flowId));
    }

    return flow.get();
  }


  private String extractArgumentValue(String argName, String[] argValues) {
    /* Currently we support only one value for each of the parameters and return an error
    otherwise.
    */
    requireNonNull(argName);
    requireNonNull(argValues);
    if (argValues.length != 1) {
      throw new RuntimeException(format("Argument %s should have exactly one value", argName));
    }
    return argValues[0];
  }


  private void setFlowCreateAndLastModifiedInfo(FlowResponse flow) {
    /* Get first flow version to find when the flow was created */
    Optional<FlowVersion> firstFlowVersion = flowVersionDao.getVersion(flow.getId(), 1);
    if (!firstFlowVersion.isPresent()) {
      throw new CloudFlowNotFoundException("First version of flow: " + flow.getId() + "does not "
          + "exist");
    }
    flow.setCreatedByUser(firstFlowVersion.get().getCreatedBy());
    flow.setCreatedOn(firstFlowVersion.get().getCreateTime());

    /* Get the last flow version that was uploaded as part of the project to extract modifiedBy
     and modifiedOn */
    Optional<FlowVersion> lastFlowVersion = flowVersionDao.getVersion(flow.getId(),
        flow.getFlowVersionCount());
    if (!lastFlowVersion.isPresent()) {
      throw new CloudFlowNotFoundException("Latest flow version of flow: " + flow.getId() + "does"
          + " not exist");
    }
    flow.setModifiedByUser(lastFlowVersion.get().getCreatedBy());
    flow.setModifiedOn(lastFlowVersion.get().getCreateTime());
  }


  private void setLastFlowVersion(FlowResponse flow) {
    Optional<FlowVersion> lastFlowVersion = flowVersionDao.getVersion(flow.getId(),
        flow.getFlowVersionCount());
    if (!lastFlowVersion.isPresent()) {
      throw new CloudFlowNotFoundException("Latest flow version of flow: " + flow.getId() + "does"
          + "not exist");
    }
    flow.setLastVersion(lastFlowVersion.get());
  }


  @Override
  public List<FlowResponse> getAllFlows(User user, Map<String, String[]> queryParamMap) {
    Optional<String> projectId = Optional.empty();
    Optional<String> projectVersion = Optional.empty();
    Optional<String> projectName = Optional.empty();

    for (Map.Entry<String, String[]> entry : queryParamMap.entrySet()) {
      String key = entry.getKey();
      String[] value = entry.getValue();
      if (key.equals(PROJECT_ID_PARAM)) {
        projectId = Optional.of(extractArgumentValue(key, value));
      } else if (key.equals(PROJECT_VERSION_PARAM)) {
        projectVersion = Optional.of(extractArgumentValue(key, value));
      } else if (key.equals(PROJECT_NAME_PARAM)) {
        projectName = Optional.of(extractArgumentValue(key, value));
      }
    }

    /* Both projectId and projectName cannot be passed as query params */
    if (projectId.isPresent() && projectName.isPresent()) {
      throw new CloudFlowException("Filtering by both project id and project name is not "
          + "supported");
    }

    /* projectVersion cannot be passed as Query param without projectId or projectName */
    if (!(projectId.isPresent() || projectName.isPresent()) && projectVersion.isPresent()) {
      throw new CloudFlowException("Project id or project name must be passed along with project "
          + "version");
    }

    List<Project> projects = new ArrayList<Project>();
    /* If projectId or projectName is present, get the project based on query params */
    if (projectId.isPresent() || projectName.isPresent()) {
      Optional<Project> project = projectDao
          .getProjectByParams(projectId, projectName, projectVersion);
      /* Throw exception if no project was found based on the query params */
      if (!project.isPresent()) {
        String errorMsg = String.format("Project record doesn't exist for query params passed");
        log.error(errorMsg);
        throw new CloudFlowNotFoundException(errorMsg);
      }
      projects.add(project.get());
    } else {
      /* No query params passed, get all projects for the user */
      projects.addAll(projectDao.getAll(user));
    }

    List<FlowResponse> flows = new ArrayList<FlowResponse>();

    /* If projectVersion is not set, get the flows for the latest project version */
    String projectVersionStr = "-1";
    if (projectVersion.isPresent()) {
      projectVersionStr = projectVersion.get();
    }

    /* For each project, get all flows and append them to a list */
    for (Project project : projects) {
      /* Get all flows for projectId and projectVersion combination */
      flows.addAll(flowDao.getAllFlows(project.getId(), projectVersionStr));

      /* For each flow, set create info, modified info and last flow version details */
      for (FlowResponse flow : flows) {
        setFlowCreateAndLastModifiedInfo(flow);
        setLastFlowVersion(flow);
      }
    }

    return flows;
  }
}
