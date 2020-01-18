package cloudflow.servlets;

import azkaban.server.HttpRequestUtils;
import azkaban.server.session.Session;
import azkaban.user.User;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import cloudflow.models.Space;
import cloudflow.services.SpaceService;
import com.linkedin.jersey.api.uri.UriTemplate;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpaceServlet extends LoginAbstractAzkabanServlet {

  private static final String GET_ALL_SPACE_URI = "/spaces";
  private static final String GET_SPACE_URI_TEMPLATE = "/spaces/{spaceId}";
  private static final String SPACE_ID_KEY = "spaceId";
  private SpaceService spaceService;
  private ObjectMapper objectMapper;

  private static final Logger log = LoggerFactory.getLogger(SpaceServlet.class);

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = (AzkabanWebServer) getApplication();
    this.spaceService = server.spaceService();
    this.objectMapper = server.objectMapper();
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws IOException {
    /* there can be two requests here */
    /* Get all records */
    if (GET_ALL_SPACE_URI.equals(req.getRequestURI())) {
      String response = objectMapper.writeValueAsString(spaceService.getAllSpaces());
      this.writeResponse(resp, response);
      return;
    }
    /* Get specific record */
    try {
      UriTemplate template = new UriTemplate(GET_SPACE_URI_TEMPLATE);
      Map<String, String> map = new HashMap<>();
      template.match(req.getRequestURI(), map);
      int spaceId = Integer.parseInt(map.get(SPACE_ID_KEY));
      String response = objectMapper.writeValueAsString(spaceService.getSpace(spaceId));
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
      Space space = objectMapper.readValue(body, Space.class);
      /* ideally the user name should be obtained from session object
       *  session.getUser()
       *  writing user name directly makes testing easier
       * */

      Space createdSpace = spaceService.create(space, new User("sarumuga"));
      resp.setStatus(HttpStatus.SC_CREATED);
    } catch(Exception e) {
      log.error("Input is likely missing something", e);
      resp.setStatus(HttpStatus.SC_BAD_REQUEST);
    }
  }
}
