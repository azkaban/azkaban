package azkaban.test.utils;

import java.io.IOException;

import javax.mail.MessagingException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import azkaban.utils.EmailMessage;

public class EmailMessageTest {

	String host = "";
	String sender = "";
	String user = "";
	String password = "";
	
	String toAddr = "";
	
	private EmailMessage em;
	@Before
	public void setUp() throws Exception {
		em = new EmailMessage(host, user, password);
		em.setFromAddress(sender);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testSendEmail() throws IOException {
		em.addToAddress(toAddr);
		//em.addToAddress("cyu@linkedin.com");
		em.setSubject("azkaban test email");
		em.setBody("azkaban test email");
		try {
			em.sendEmail();
		} catch (MessagingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
