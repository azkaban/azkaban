package azkaban.trigger;

public enum TriggerStatus {
	READY(10), PAUSED(20), EXPIRED(30);
	
	private int numVal;

	TriggerStatus(int numVal) {
		this.numVal = numVal;
	}

	public int getNumVal() {
		return numVal;
	}
	
	public static TriggerStatus fromInteger(int x) {
		switch (x) {
		case 10:
			return READY;
		case 20:
			return PAUSED;
		case 30:
			return EXPIRED;
		default:
			return READY;
		}
	}

}
