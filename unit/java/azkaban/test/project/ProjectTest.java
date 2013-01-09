package azkaban.test.project;

import static org.junit.Assert.*;

import org.junit.Test;

import azkaban.project.Project;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.utils.JSONUtils;

public class ProjectTest {
    @Test
    public void testToAndFromObject() throws Exception {
    	Project project = new Project(1, "tesTing");
    	project.setCreateTimestamp(1l);
    	project.setLastModifiedTimestamp(2l);
    	project.setDescription("I am a test");
    	project.setUserPermission("user1", new Permission(new Type[]{Type.ADMIN, Type.EXECUTE}));
    	
    	Object obj = project.toObject();
    	String json = JSONUtils.toJSON(obj);
    	
    	Object jsonObj = JSONUtils.parseJSONFromString(json);

    	Project parsedProject = Project.projectFromObject(jsonObj);
    	
    	assertTrue(project.equals(parsedProject));
    }

}
