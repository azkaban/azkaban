package azkaban.imagemgmt.servlets;

import azkaban.server.HttpRequestUtils;
import azkaban.server.session.Session;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import azkaban.imagemgmt.exeception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.RequestContext;
import azkaban.imagemgmt.services.ImageVersionService;
import com.linkedin.jersey.api.uri.UriTemplate;
import azkaban.imagemgmt.utils.ImageMgmtConstants;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.HttpStatus;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImageVersionServlet extends LoginAbstractAzkabanServlet {
  private static final String GET_IMAGE_VERSION_URI = "/imageVersions";
  private static final String IMAGE_VERSION_ID_KEY = "imageVersionId";
  private static final UriTemplate CREATE_IMAGE_VERSION_URI_TEMPLATE = new UriTemplate(
      String.format("/imageVersions/{%s}", IMAGE_VERSION_ID_KEY));
  private ImageVersionService imageVersionService;
  private ObjectMapper objectMapper;

  private static final Logger log = LoggerFactory.getLogger(ImageVersionServlet.class);

  public ImageVersionServlet() {
    super(new ArrayList<>());
  }

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = (AzkabanWebServer) getApplication();
    this.objectMapper = server.getObjectMapper();
    this.imageVersionService = server.getImageVersionsService();
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException, IOException {
    /* Get specific record */
    try {
      String response = null;
      if(GET_IMAGE_VERSION_URI.equals(req.getRequestURI())) {
        String imageType = req.getParameter(ImageMgmtConstants.IMAGE_TYPE);
        String imageVersion = req.getParameter(ImageMgmtConstants.IMAGE_VERSION);
        RequestContext requestContext = RequestContext.newBuilder()
            .addNonNullParam(ImageMgmtConstants.IMAGE_TYPE, imageType)
            .addNonNullParam(ImageMgmtConstants.IMAGE_VERSION, imageVersion)
            .build();

        response =
            objectMapper.writeValueAsString(
                imageVersionService.getImageVersion(requestContext));

      }
      this.writeResponse(resp, response);
    } catch(Exception e) {
      log.error("Requested image metadata not found " + e);
      resp.setStatus(HttpStatus.SC_NOT_FOUND);
    }
  }

  @Override
  protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException, IOException {
    try {
      String jsonPayload = HttpRequestUtils.getBody(req);
      RequestContext requestContext = RequestContext.newBuilder()
          .jsonPayload(jsonPayload)
          .user(session.getUser().getUserId())
          .build();
      Integer imageVersionId = imageVersionService.createImageVersion(requestContext);
      resp.setStatus(HttpStatus.SC_CREATED);
      resp.setHeader("Location", CREATE_IMAGE_VERSION_URI_TEMPLATE.createURI(imageVersionId.toString()));
      sendResponse(resp, HttpServletResponse.SC_CREATED, new HashMap<>());
    } catch (final ImageMgmtValidationException e) {
      log.error("Input for creating image version metadata is invalid", e);
      sendErrorResponse(resp, HttpServletResponse.SC_BAD_REQUEST,
          "Bad request for creating image version metadata");
    } catch (final Exception e) {
      log.error("Exception while creating image version metadata", e);
      sendErrorResponse(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "Exception while creating image version metadata. "+e.getMessage());
    }
  }
}
