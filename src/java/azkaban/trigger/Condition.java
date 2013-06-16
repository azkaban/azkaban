package azkaban.trigger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.log4j.Logger;


public class Condition {
	
	private static Logger logger = Logger.getLogger(Condition.class);
	
	private static JexlEngine jexl = new JexlEngine();
	private static CheckerTypeLoader checkerLoader = null;
	private Expression expression;
	private Map<String, ConditionChecker> checkers = new HashMap<String, ConditionChecker>();
	private MapContext context = new MapContext();
	
	public Condition(Map<String, ConditionChecker> checkers, String expr) {
		setCheckers(checkers);
		this.expression = jexl.createExpression(expr);
	}
	
	public synchronized static void setJexlEngine(JexlEngine jexl) {
		Condition.jexl = jexl;
	}
	
	public synchronized static void setCheckerLoader(CheckerTypeLoader loader) {
		Condition.checkerLoader = loader;
	}
	
	public void registerChecker(ConditionChecker checker) {
		checkers.put(checker.getId(), checker);
		context.set(checker.getId(), checker);
	}
	
	public Map<String, ConditionChecker> getCheckers() {
		return this.checkers;
	}
	
	public void setCheckers(Map<String, ConditionChecker> checkers){
		this.checkers = checkers;
		for(ConditionChecker checker : checkers.values()) {
			this.context.set(checker.getId(), checker);
		}
	}
	
	public void resetCheckers() {
		for(ConditionChecker checker : checkers.values()) {
			checker.reset();
		}
	}
	
	public String getExpression() {
		return this.expression.getExpression();
	}
	
	public void setExpression(String expr) {
		this.expression = jexl.createExpression(expr);
	}
	
	public boolean isMet() {
		return expression.evaluate(context).equals(Boolean.TRUE);
	}
	
	public Object toJson() {
		Map<String, Object> jsonObj = new HashMap<String, Object>();
		jsonObj.put("expression", expression.getExpression());
		
		List<Object> checkersJson = new ArrayList<Object>();
		for(ConditionChecker checker : checkers.values()) {
			Map<String, Object> oneChecker = new HashMap<String, Object>();
			oneChecker.put("type", checker.getType());
			oneChecker.put("checkerJson", checker.toJson());
			checkersJson.add(oneChecker);
		}
		jsonObj.put("checkers", checkersJson);
		
		return jsonObj;
	}

	@SuppressWarnings("unchecked")
	public static Condition fromJson(Object obj) {
		Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
		Condition cond = null;
		
		try {
			Map<String, ConditionChecker> checkers = new HashMap<String, ConditionChecker>();
			List<Object> checkersJson = (List<Object>) jsonObj.get("checkers");			
			for(Object oneCheckerJson : checkersJson) {
				Map<String, Object> oneChecker = (HashMap<String, Object>) oneCheckerJson;
				String type = (String) oneChecker.get("type");
				ConditionChecker ck = checkerLoader.createCheckerFromJson(type, oneChecker.get("checkerJson"));
				checkers.put(ck.getId(), ck);
			}
				String expr = (String) jsonObj.get("expression");
				
				cond = new Condition(checkers, expr);
			
		} catch(Exception e) {
			e.printStackTrace();
			logger.error("Failed to recreate condition from json.", e);
			return null;
		}
		
		return cond;
	}
	

}
