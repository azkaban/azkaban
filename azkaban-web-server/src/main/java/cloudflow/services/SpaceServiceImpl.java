package cloudflow.services;

import azkaban.user.User;
import cloudflow.daos.SpaceDao;
import cloudflow.models.Space;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class SpaceServiceImpl implements SpaceService {

  private final SpaceDao spaceDao;
  private static final Logger log = LoggerFactory.getLogger(SpaceServiceImpl.class);

  @Inject
  public SpaceServiceImpl(SpaceDao spaceDao) {
    this.spaceDao = spaceDao;
  }

  @Override
  public int createSpace(Space space, User user) {
    int spaceId = spaceDao.create(space, user);
    return spaceId;
  }

  @Override
  public Space getSpace(int spaceId) {

    Optional<Space> space = spaceDao.get(spaceId);
    /* Not the best exception to throw but
       serves the use-case here.
     */
    if (!space.isPresent()) {
      log.error("Space record doesn't exist for the spaceId: " + spaceId);
      throw new NoSuchElementException();
    }
    return space.get();
  }

  @Override
  public List<Space> getAllSpaces() {
    return spaceDao.getAll();
  }
}
