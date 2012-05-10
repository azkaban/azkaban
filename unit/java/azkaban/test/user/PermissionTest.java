package azkaban.test.user;

import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import azkaban.user.Permission;
import azkaban.user.Permission.Type;

public class PermissionTest {
    @Before
    public void setUp() throws Exception {
    }
    
    @After
    public void tearDown() throws Exception {
    }
    
    @Test
    public void testEmptyPermissionCreation() throws Exception {
    	Permission permission = new Permission();
    	permission.setPermissionsByName(new String[]{});
    }
    
    @Test
    public void testSinglePermissionCreation() throws Exception {
    	Permission perm1 = new Permission();
    	perm1.setPermissionsByName("READ");
    	
    	Permission perm2 = new Permission();
    	perm2.setPermission(Type.READ);
    	info("Compare " + perm1.toString() + " and " + perm2.toString());
    	assertTrue(perm1.equals(perm2));
    }

    @Test
    public void testListPermissionCreation() throws Exception {
    	Permission perm1 = new Permission();
    	perm1.setPermissionsByName(new String[]{"READ", "EXECUTE"});
    	
    	Permission perm2 = new Permission();
    	perm2.setPermission(new Type[]{Type.EXECUTE, Type.READ});
    	info("Compare " + perm1.toString() + " and " + perm2.toString());
    	assertTrue(perm1.equals(perm2));
    }
    
    @Test
    public void testRemovePermission() throws Exception {
    	Permission perm1 = new Permission();
    	perm1.setPermissionsByName(new String[]{"READ", "EXECUTE", "WRITE"});
    	perm1.unsetPermissions(Type.EXECUTE);
    	
    	Permission perm2 = new Permission();
    	perm2.setPermission(new Type[]{Type.READ, Type.WRITE});
    	info("Compare " + perm1.toString() + " and " + perm2.toString());
    	assertTrue(perm1.equals(perm2));
    }
    
    @Test
    public void testRemovePermissionByName() throws Exception {
    	Permission perm1 = new Permission();
    	perm1.setPermissionsByName(new String[]{"READ", "EXECUTE", "WRITE"});
    	perm1.unsetPermissionsByName("EXECUTE");
    	
    	Permission perm2 = new Permission();
    	perm2.setPermission(new Type[]{Type.READ, Type.WRITE});
    	info("Compare " + perm1.toString() + " and " + perm2.toString());
    	assertTrue(perm1.equals(perm2));
    }
    
    @Test
    public void testToAndFromObject() throws Exception {
    	Permission permission = new Permission();
    	permission.setPermissionsByName(new String[]{"READ", "EXECUTE", "WRITE"});
    	
    	String[] array = permission.toStringArray();
    	Permission permission2 = new Permission();
    	permission2.setPermissionsByName(array);
    	assertTrue(permission.equals(permission2));
    }
    
    /**
     * Why? because it's quicker.
     * @param message
     */
    public void info(String message) {
    	System.out.println(message);
    }
}