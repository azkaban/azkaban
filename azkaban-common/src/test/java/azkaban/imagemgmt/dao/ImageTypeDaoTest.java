package azkaban.imagemgmt.dao;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import azkaban.db.DatabaseOperator;
import azkaban.imagemgmt.daos.ImageTypeDao;
import azkaban.imagemgmt.daos.ImageTypeDaoImpl;
import azkaban.imagemgmt.daos.ImageTypeDaoImpl.FetchImageTypeHandler;
import azkaban.imagemgmt.exception.ImageMgmtDaoException;
import azkaban.imagemgmt.exception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageOwnership;
import azkaban.imagemgmt.models.ImageOwnership.Role;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.models.ImageType.Deployable;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.springframework.util.Assert;

public class ImageTypeDaoTest {

  private ImageTypeDao imageTypeDaoMock;
  private DatabaseOperator databaseOperatorMock;

  @Before
  public void setup() {
    databaseOperatorMock = mock(DatabaseOperator.class);
    imageTypeDaoMock = new ImageTypeDaoImpl(databaseOperatorMock);
  }

  @Test
  public void testGetImageTypeWithOwnershipsByName() throws Exception {
    List<ImageType> its = getImageTypeList();
    String name = "imageName";
    when(databaseOperatorMock.query(anyString(), any(FetchImageTypeHandler.class),
        anyString())).thenReturn(its);
    java.util.Optional<ImageType> imageType = imageTypeDaoMock
        .getImageTypeWithOwnershipsByName(name);
    assert (imageType.isPresent());
    assert (imageType.get().equals(its.get(0)));

  }

  @Test(expected = ImageMgmtDaoException.class)
  public void testGetImageTypeWithOwnershipsByNameFailsWithMultipleImageTypes() throws Exception {
    List<ImageType> its = getImageTypeList();
    ImageType it2 = new ImageType();
    its.add(it2);
    String name = "imageName";
    when(databaseOperatorMock.query(anyString(), any(FetchImageTypeHandler.class),
        anyString())).thenReturn(its);
    java.util.Optional<ImageType> imageType = imageTypeDaoMock
        .getImageTypeWithOwnershipsByName(name);
  }

  @Test
  public void testGetImageTypeWithOwnershipsById() throws Exception {
    List<ImageType> its = getImageTypeList();
    String id = "1";
    when(databaseOperatorMock.query(anyString(), any(FetchImageTypeHandler.class),
        anyString())).thenReturn(its);
    ImageType imageType = imageTypeDaoMock.getImageTypeWithOwnershipsById(id);
    Assert.notNull(imageType);
    assert imageType.getName().equals("name");
  }

  @Test(expected = ImageMgmtDaoException.class)
  public void testGetImageTypeWithOwnershipsByIdWhenFetchReturnsNull() throws Exception {
    String id = "1";
    when(databaseOperatorMock.query(anyString(), any(FetchImageTypeHandler.class),
        anyString())).thenReturn(null);
    imageTypeDaoMock.getImageTypeWithOwnershipsById(id);
  }

  private List<ImageType> getImageTypeList() {
    List<ImageType> its = new ArrayList<ImageType>();
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
    return its;
  }

}
