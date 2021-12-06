package azkaban.imagemgmt.servlets;

import azkaban.imagemgmt.daos.HPFlowDao;
import azkaban.imagemgmt.dto.HPFlowDTO;
import azkaban.imagemgmt.exception.ErrorCode;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.exception.ImageMgmtInvalidInputException;
import azkaban.imagemgmt.exception.ImageMgmtInvalidPermissionException;
import azkaban.imagemgmt.exception.ImageMgmtValidationException;
import azkaban.imagemgmt.permission.PermissionManager;
import azkaban.imagemgmt.services.HPFlowService;
import azkaban.imagemgmt.utils.ConverterUtils;
import azkaban.server.HttpRequestUtils;
import azkaban.server.session.Session;
import azkaban.user.User;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;


/**
 * This class handles all the REST API calls for API "/hpFlows"
 * It currently implements addition of high priority flows.
 */
public class HPFlowServlet extends LoginAbstractAzkabanServlet {

  private static final Logger log = Logger.getLogger(HPFlowServlet.class);
  private static final String BASE_HP_FLOW_URI = "/hpFlows";
  private ConverterUtils converterUtils;
  private HPFlowService hpFlowService;

  public HPFlowServlet() {
    super(new ArrayList<>());
  }

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = getApplication();
    this.converterUtils = server.getConverterUtils();
    this.hpFlowService = server.getHPFlowService();
  }

  @Override
  protected void handleGet(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session)
    throws ServletException, IOException {

  }

  @Override
  protected void handlePost(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session)
      throws ServletException, IOException {
    handleAddHPFlows(req, resp, session);
  }

  /**
   * This method takes in a CSV of high priority flows and adds them to metadata.
   * @param req
   * @param resp
   * @param session
   * @throws ServletException
   * @throws IOException
   */
  protected void handleAddHPFlows(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session)
      throws ServletException, IOException {
    try {
      final String jsonPayLoad = HttpRequestUtils.getBody(req);
      final HPFlowDTO hpFlowDTO = this.converterUtils.convertToDTO(jsonPayLoad, HPFlowDTO.class);
      // If there are no flow IDs in the list return with error
      if (hpFlowDTO.getFlowIds().isEmpty()) {
        log.error("There are no flow IDs provided");
        throw new ImageMgmtValidationException(ErrorCode.BAD_REQUEST,
            "Required field flow IDs is empty");
      }
      // Check for required permission to invoke the API
      final User user = session.getUser();
      if (!hasHPFlowManagementPermission(user)) {
        log.debug(String.format("Invalid permission to access High Priority "
            + "flows for user: %s", user.getUserId()));
        throw new ImageMgmtInvalidPermissionException(ErrorCode.FORBIDDEN,
            "Invalid permission to manage high priority flows");
      }
      // Set the user who invoked the API
      hpFlowDTO.setCreatedBy(user.getUserId());
      // Add the HP flow metadata
      this.hpFlowService.addHPFlows(hpFlowDTO);
      // Prepare to send response
      resp.setStatus(HttpStatus.SC_CREATED);
      sendResponse(resp, HttpServletResponse.SC_CREATED, new HashMap<>());
    } catch (final ImageMgmtException e) {
      log.error("Exception while adding high priority flows.", e);
      sendErrorResponse(resp, e.getErrorCode().getCode(), e.getMessage());
    } catch (final Exception e) {
      log.error("Exception while adding high priority flows", e);
      sendErrorResponse(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "Exception while adding high priority flows. " + e.getMessage());
    }
  }

  /**
   * Checks if the provided user has access to manage high priority flows.
   * @param user
   * @return
   */
  private boolean hasHPFlowManagementPermission(final User user) {
    /**
     * Azkaban ADMIN role must have full permission to access image management APIs. Hence, no
     * further permission check is required.
     */
    if (isAzkabanAdmin(user)) {
      return true;
    }

    // Check the HPFlow Management API's access permission for other users.
    final PermissionManager permissionManager = getApplication().getPermissionManager();
    return permissionManager.hasPermission(user.getUserId());
  }
}
