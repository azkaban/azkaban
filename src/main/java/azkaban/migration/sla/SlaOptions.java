package azkaban.migration.sla;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import azkaban.migration.sla.SLA.SlaSetting;

@Deprecated
public class SlaOptions {

	public List<String> getSlaEmails() {
		return slaEmails;
	}
	public void setSlaEmails(List<String> slaEmails) {
		this.slaEmails = slaEmails;
	}
	public List<SlaSetting> getSettings() {
		return settings;
	}
	public void setSettings(List<SlaSetting> settings) {
		this.settings = settings;
	}
	private List<String> slaEmails;
	private List<SlaSetting> settings;
	public Object toObject() {
		Map<String, Object> obj = new HashMap<String, Object>();
		obj.put("slaEmails", slaEmails);
		List<Object> slaSettings = new ArrayList<Object>();
		for(SlaSetting s : settings) {
			slaSettings.add(s.toObject());
		}
		obj.put("settings", slaSettings);
		return obj;
	}
	@SuppressWarnings("unchecked")
	public static SlaOptions fromObject(Object object) {
		if(object != null) {
			SlaOptions slaOptions = new SlaOptions();
			Map<String, Object> obj = (HashMap<String, Object>) object;
			slaOptions.setSlaEmails((List<String>) obj.get("slaEmails"));
			List<SlaSetting> slaSets = new ArrayList<SlaSetting>();
			for(Object set: (List<Object>)obj.get("settings")) {
				slaSets.add(SlaSetting.fromObject(set));
			}
			slaOptions.setSettings(slaSets);
			return slaOptions;
		}
		return null;			
	}
}