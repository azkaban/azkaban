package cloudflow.servlets;

import azkaban.server.HttpRequestUtils;
import azkaban.server.session.Session;
import azkaban.user.User;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import cloudflow.services.ProjectService;
import cloudflow.webapp.controllers.ProjectController;
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

  private static final String GET_ALL_PROJECT_URI = "/projects";
  private static final String GET_PROJECT_URI_TEMPLATE = "/projects/{projectId}";
  private static final String PROJECT_ID_KEY = "projectId";
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
      throws IOException {
    /* there can be two requests here */
    /* Get all records */
    if (GET_ALL_PROJECT_URI.equals(req.getRequestURI())) {
      String response = objectMapper.writeValueAsString(projectService.getAll());
      this.writeResponse(resp, response);
      return;
    }

    /* Get specific record */
    try {
      UriTemplate template = new UriTemplate(GET_PROJECT_URI_TEMPLATE);
      Map<String, String> map = new HashMap<>();
      template.match(req.getRequestURI(), map);
      String projectId = map.get(PROJECT_ID_KEY);
      String response = objectMapper.writeValueAsString(projectService.get(projectId));
      this.writeResponse(resp, response);
    } catch(Exception e) {
      log.error("Content is likely not present " + e);
      resp.setStatus(HttpStatus.SC_NOT_FOUND);
    }
  }

  @Override
  protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException, IOException {
    try {
      String body = HttpRequestUtils.getBody(req);
      Project project = objectMapper.readValue(body, Project.class);
      /* ideally the user name should be obtained from session object
       *  session.getUser()
       *  writing user name directly makes testing easier
       * */

      String projectId = projectService.create(project, new User("gsalia"));
      // TODO: set the projectId in response body
      resp.setStatus(HttpStatus.SC_CREATED);
    } catch(Exception e) {
      log.error("Input is likely missing something", e);
      resp.setStatus(HttpStatus.SC_BAD_REQUEST);
    }
  }

}
