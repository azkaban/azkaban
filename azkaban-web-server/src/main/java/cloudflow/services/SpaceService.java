package cloudflow.services;

import azkaban.user.User;
import cloudflow.models.Space;
import java.util.List;

public interface SpaceService {
  int createSpace(Space space, User user);
  Space getSpace(int spaceId);
  List<Space> getAllSpaces();
}
