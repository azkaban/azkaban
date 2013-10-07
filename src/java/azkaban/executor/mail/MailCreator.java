package azkaban.executor.mail;

import azkaban.executor.ExecutableFlow;
import azkaban.utils.EmailMessage;

public interface MailCreator {
	public boolean createFirstErrorMessage(ExecutableFlow flow, EmailMessage message, String azkabanName, String clientHostname, String clientPortNumber, String... vars);
	public boolean createErrorEmail(ExecutableFlow flow, EmailMessage message, String azkabanName, String clientHostname, String clientPortNumber, String... vars);
	public boolean createSuccessEmail(ExecutableFlow flow, EmailMessage message, String azkabanName, String clientHostname, String clientPortNumber, String... vars);
}
