package cloudflow.daos;

import azkaban.user.User;
import cloudflow.models.Space;
import java.util.List;
import java.util.Optional;

public interface SpaceDao {
  int create(Space space, User user);
  Optional<Space> get(int spaceId);
  List<Space> getAll();
}
