package azkaban.imagemgmt.servlets;

import azkaban.server.HttpRequestUtils;
import azkaban.server.session.Session;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import com.linkedin.jersey.api.uri.UriTemplate;
import azkaban.imagemgmt.exeception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.RequestContext;
import azkaban.imagemgmt.services.ImageTypeService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.HttpStatus;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImageTypeServlet extends LoginAbstractAzkabanServlet {

  private static final String GET_IMAGE_TYPE_URI = "/imageTypes";
  private static final String IMAGE_TYPE_ID_KEY = "imageTypeId";
  private static final UriTemplate CREATE_IMAGE_TYPE_URI_TEMPLATE = new UriTemplate(
      String.format("/imageTypes/{%s}", IMAGE_TYPE_ID_KEY));
  private ImageTypeService imageTypeService;
  private ObjectMapper objectMapper;

  private static final Logger log = LoggerFactory.getLogger(ImageTypeServlet.class);

  public ImageTypeServlet() {
    super(new ArrayList<>());
  }

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = (AzkabanWebServer) getApplication();
    this.objectMapper = server.getObjectMapper();
    this.imageTypeService = server.getImageTypeService();
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException, IOException {
    /* Get specific record */
    try {
      UriTemplate template = new UriTemplate(GET_IMAGE_TYPE_URI);
      Map<String, String> map = new HashMap<>();
      template.match(req.getRequestURI(), map);
      String imageType = req.getParameter("imageType");
      String imageVersion = req.getParameter("imageVersion");
      /*String response =
          objectMapper.writeValueAsString(
              imageTypeService.getImageVersion(imageType, imageVersion));*/
      System.out.println("Image version : " + imageVersion);
      System.out.println("Image type : " + imageType);
      log.info("imageVersion : {} ", imageVersion);
      log.info("imageType : {} ", imageType);
      //this.writeResponse(resp, response);
    } catch (Exception e) {
      log.error("Content is likely not present " + e);
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
      Integer imageTypeId = imageTypeService.createImageType(requestContext);
      resp.setStatus(HttpStatus.SC_CREATED);
      resp.setHeader("Location", CREATE_IMAGE_TYPE_URI_TEMPLATE.createURI(imageTypeId.toString()));
      sendResponse(resp, HttpServletResponse.SC_CREATED, new HashMap<>());
    } catch (final ImageMgmtValidationException e) {
      log.error("Input for creating image type is invalid", e);
      sendErrorResponse(resp, HttpServletResponse.SC_BAD_REQUEST,
          "Bad request for creating an image type");
    } catch (final Exception e) {
      log.error("Exception while creating an image type", e);
      sendErrorResponse(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "Exception while creating an image type");
    }
  }
}
