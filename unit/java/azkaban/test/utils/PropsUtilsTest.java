package azkaban.test.utils;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.UndefinedPropertyException;

public class PropsUtilsTest {
	@Test
	public void testGoodResolveProps() throws IOException {
		Props propsGrandParent = new Props();
		Props propsParent = new Props(propsGrandParent);
		Props props = new Props(propsParent);
		
		// Testing props in general
		props.put("letter", "a");
		propsParent.put("letter", "b");
		propsGrandParent.put("letter", "c");
	
		Assert.assertEquals("a", props.get("letter"));
		propsParent.put("my", "name");
		propsParent.put("your", "eyes");
		propsGrandParent.put("their", "ears");
		propsGrandParent.put("your", "hair");
		
		Assert.assertEquals("name", props.get("my"));
		Assert.assertEquals("eyes", props.get("your"));
		Assert.assertEquals("ears", props.get("their"));
		
		props.put("res1", "${my}");
		props.put("res2", "${their} ${letter}");
		props.put("res7", "${my} ${res5}");
		
		propsParent.put("res3", "${your} ${their} ${res4}");
		propsGrandParent.put("res4", "${letter}");
		propsGrandParent.put("res5", "${their}");
		propsParent.put("res6", " t ${your} ${your} ${their} ${res5}");
		
		Props resolved = PropsUtils.resolveProps(props);
		Assert.assertEquals("name", resolved.get("res1"));
		Assert.assertEquals("ears a", resolved.get("res2"));
		Assert.assertEquals("eyes ears a", resolved.get("res3"));
		Assert.assertEquals("a", resolved.get("res4"));
		Assert.assertEquals("ears", resolved.get("res5"));
		Assert.assertEquals(" t eyes eyes ears ears", resolved.get("res6"));
		Assert.assertEquals("name ears", resolved.get("res7"));
	}
	
	@Test
	public void testCyclesResolveProps() throws IOException {
		Props propsGrandParent = new Props();
		Props propsParent = new Props(propsGrandParent);
		Props props = new Props(propsParent);
		
		// Testing props in general
		props.put("a", "${a}");
		failIfNotException(props);
		
		props.put("a", "${b}");
		props.put("b", "${a}");
		failIfNotException(props);
		
		props.clearLocal();
		props.put("a", "${b}");
		props.put("b", "${c}");
		propsParent.put("d", "${a}");
		failIfNotException(props);
		
		props.clearLocal();
		props.put("a", "testing ${b}");
		props.put("b", "${c}");
		propsGrandParent.put("c", "${d}");
		propsParent.put("d", "${a}");
		failIfNotException(props);
		
		props.clearLocal();
		props.put("a", "testing ${c} ${b}");
		props.put("b", "${c} test");
		propsGrandParent.put("c", "${d}");
		propsParent.put("d", "${a}");
		failIfNotException(props);
	}
	
	private void failIfNotException(Props props) {
		try {
			Props resolved = PropsUtils.resolveProps(props);
			Assert.fail();
		}
		catch (UndefinedPropertyException e) {
			e.printStackTrace();
		}
		catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
	}
}
