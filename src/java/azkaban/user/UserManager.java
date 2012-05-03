package azkaban.user;

/**
 * Interface for the UserManager. Implementors will have to handle the retrieval
 * of the User object given the username and password.
 * 
 * The constructor will be called with a azkaban.utils.Props object passed as the only
 * parameter. If such a constructor doesn't exist, than the UserManager instantiation may
 * fail.
 */
public interface UserManager {
	/**
	 * Retrieves the user given the username and password to authenticate against.
	 * 
	 * @param username
	 * @param password
	 * @return
	 * @throws UserManagerException If the username/password combination doesn't exist.
	 */
	public User getUser(String username, String password) throws UserManagerException;
}
