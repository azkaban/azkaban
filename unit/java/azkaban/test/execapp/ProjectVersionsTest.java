package azkaban.test.execapp;

import java.util.ArrayList;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import azkaban.execapp.ProjectVersion;

public class ProjectVersionsTest {
	
	@Test
	public void testVersionOrdering() {
		ArrayList<ProjectVersion> pversion = new ArrayList<ProjectVersion>();
		pversion.add(new ProjectVersion(1, 2));
		pversion.add(new ProjectVersion(1, 3));
		pversion.add(new ProjectVersion(1, 1));
		
		Collections.sort(pversion);
		
		int i = 0;
		for (ProjectVersion version: pversion) {
			Assert.assertTrue(i < version.getVersion());
			i = version.getVersion();
		}
	}
}