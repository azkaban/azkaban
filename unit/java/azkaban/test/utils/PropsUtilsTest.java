package azkaban.test.utils;

import java.io.IOException;

import org.junit.Assert;
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
	public void testExpressionResolution() throws IOException {
		Props props = Props.of(
			"normkey", "normal",
			"num1", "1",
			"num2", "2",
			"num3", "3",
			"variablereplaced", "${num1}",
			"expression1", "$(1+10)",
			"expression2", "$(1+10)*2",
			"expression3", "$((${num1} + ${num3})*10)",
			"expression4", "$(${num1} + ${expression3})",
			"expression5", "$($($(2+3)) + 3) + $(${expression3} + 1))",
			"expression6", "$(1 + ${normkey}))"
		);

		Props resolved = PropsUtils.resolveProps(props);
		Assert.assertEquals("normal", resolved.get("normkey"));
		Assert.assertEquals("1", resolved.get("num1"));
		Assert.assertEquals("2", resolved.get("num2"));
		Assert.assertEquals("3", resolved.get("num3"));
		Assert.assertEquals("1", resolved.get("variablereplaced"));
		Assert.assertEquals("11", resolved.get("expression1"));
		Assert.assertEquals("11*2", resolved.get("expression2"));
		Assert.assertEquals("40", resolved.get("expression3"));
		Assert.assertEquals("41", resolved.get("expression4"));
		Assert.assertEquals("11 + 41", resolved.get("expression5"));
		Assert.assertEquals("1 + normal", resolved.get("expression6"));
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
			PropsUtils.resolveProps(props);
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
