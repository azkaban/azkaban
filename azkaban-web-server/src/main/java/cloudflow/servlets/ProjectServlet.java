package cloudflow.servlets;

import azkaban.server.HttpRequestUtils;
import azkaban.server.session.Session;
import azkaban.user.User;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import cloudflow.services.ProjectService;
import cloudflow.models.Project;
import com.linkedin.jersey.api.uri.UriTemplate;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.HttpStatus;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProjectServlet extends LoginAbstractAzkabanServlet {

  private static final String ALL_PROJECT_URI = "/projects";
  private static final String PROJECT_ID_KEY = "projectId";
  private static final UriTemplate GET_PROJECT_URI_TEMPLATE = new UriTemplate(
      String.format("/projects/{%s}", PROJECT_ID_KEY));

  private ProjectService projectService;
  private ObjectMapper objectMapper;

  private static final Logger log = LoggerFactory.getLogger(ProjectServlet.class);

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = (AzkabanWebServer) getApplication();
    this.projectService = server.projectService();
    this.objectMapper = server.objectMapper();
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws IOException, ServletException {
    Map<String, String> templateVariableToValue = new HashMap<>();
    if (ALL_PROJECT_URI.equals(req.getRequestURI())) {
      /* Get all records */
      sendResponse(resp, HttpServletResponse.SC_OK,
          projectService.getAllProjects(session.getUser()));
      return;
    } else if (GET_PROJECT_URI_TEMPLATE.match(req.getRequestURI(), templateVariableToValue)) {
      /* Get specific record */
      try {
        String projectId = templateVariableToValue.get(PROJECT_ID_KEY);

        /* Validate projectId is an Integer. */
        try {
          Integer.parseInt(projectId);
        } catch (NumberFormatException e) {
          log.error("Invalid project id: ", projectId);
          sendErrorResponse(resp, HttpServletResponse.SC_BAD_REQUEST, "Invalid project id");
          return;
        }
        sendResponse(resp, HttpServletResponse.SC_OK, projectService.getProject(projectId));
      } catch (Exception e) {
        log.error("Exception while fetching project: " + e);
        sendErrorResponse(resp, HttpServletResponse.SC_NOT_FOUND, "Project id not found");
        return;
      }
    } else {
      /* Unsupported route, return an error */
      log.error("Invalid route for projects endpoint: " + req.getRequestURI());
      sendErrorResponse(resp, HttpServletResponse.SC_NOT_FOUND, "Unsupported projects API "
          + "endpoint");
      return;
    }
  }

  @Override
  protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException, IOException {
    if (ALL_PROJECT_URI.equals(req.getRequestURI())) {
      try {
        String body = HttpRequestUtils.getBody(req);
        Project project = objectMapper.readValue(body, Project.class);

        String projectId = projectService.createProject(project, session.getUser());
        resp.setHeader("Location", "/projects/" + projectId);
        sendResponse(resp, HttpServletResponse.SC_CREATED, "");
        return;
      } catch (Exception e) {
        log.error("Error while handling POST request for projects. Got Exception: " + e);
        sendErrorResponse(resp, HttpServletResponse.SC_BAD_REQUEST, "BAD request to create project");
        return;
      }
    } else {
      /* Unsupported route, return an error */
      log.error("Invalid route for projects endpoint: " + req.getRequestURI());
      sendErrorResponse(resp, HttpServletResponse.SC_NOT_FOUND, "Unsupported projects API "
          + "endpoint");
      return;
    }
  }

}
