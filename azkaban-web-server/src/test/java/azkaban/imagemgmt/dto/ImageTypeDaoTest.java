package azkaban.imagemgmt.dto;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import azkaban.db.DatabaseOperator;
import azkaban.imagemgmt.daos.ImageTypeDao;
import azkaban.imagemgmt.daos.ImageTypeDaoImpl;
import azkaban.imagemgmt.daos.ImageTypeDaoImpl.FetchImageTypeHandler;
import azkaban.imagemgmt.models.ImageOwnership;
import azkaban.imagemgmt.models.ImageOwnership.Role;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.models.ImageType.Deployable;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class ImageTypeDaoTest {

  private final ImageTypeDao imageTypeDaoMock = mock(ImageTypeDaoImpl.class);
  private final DatabaseOperator databaseOperator = mock(DatabaseOperator.class);

  @Test
  public void testGetImageTypeWithOwnershipsByName() throws Exception {
    List<ImageType> its = new ArrayList<ImageType>() {
    };
    ImageType it = new ImageType();
    it.setName("name");
    it.setDescription("description");
    it.setDeployable(Deployable.IMAGE);
    ImageOwnership io = new ImageOwnership();
    io.setOwner("owner");
    io.setName("name");
    io.setRole(Role.ADMIN);
    List<ImageOwnership> ios = new ArrayList<>();
    ios.add(io);
    it.setOwnerships(ios);
    its.add(it);
    when(this.databaseOperator.query(any(String.class), any(FetchImageTypeHandler.class),
        any(String.class))).thenReturn(its);
    imageTypeDaoMock.getImageTypeWithOwnershipsByName(any(String.class));
  }


  private Deployable deployable;
  // Associated ownership information for the image type
  private List<ImageOwnership> ownerships;


  public void setDeployable(Deployable deployable) {
    this.deployable = deployable;
  }

  public List<ImageOwnership> getOwnerships() {
    return ownerships;
  }

  public void setOwnerships(List<ImageOwnership> ownerships) {
    this.ownerships = ownerships;
  }

}
