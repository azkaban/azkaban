package azkaban.alert;

import azkaban.executor.ExecutableFlow;
import azkaban.sla.SlaOption;

public interface Alerter {
	void alertOnSuccess(ExecutableFlow exflow) throws Exception;
	void alertOnError(ExecutableFlow exflow, String ... extraReasons) throws Exception;
	void alertOnFirstError(ExecutableFlow exflow) throws Exception;
	void alertOnSla(SlaOption slaOption, String slaMessage) throws Exception;
}
