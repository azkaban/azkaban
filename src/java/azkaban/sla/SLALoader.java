package azkaban.sla;

import java.util.List;

public interface SLALoader {

	public void insertSLA(SLA s) throws SLAManagerException;
	
	public List<SLA> loadSLAs() throws SLAManagerException;
	
	public void removeSLA(SLA s) throws SLAManagerException;

//	public void updateSLA(SLA s) throws SLAManagerException;
}
