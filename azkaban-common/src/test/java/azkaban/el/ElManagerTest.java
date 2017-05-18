package azkaban.el;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.junit.Test;


public class ElManagerTest {

    @Test
    public void testDefaultEl() throws Exception {
        int year = BasicFunction.year();

        JexlEngine je = new JexlEngine();
        je.setFunctions(ElManager.getFunctionMap());
        String expression = "year()";
        Expression e =je.createExpression(expression);
        int elYear = (Integer) (e.evaluate(new MapContext()));
        assert elYear== year;
    }
}
