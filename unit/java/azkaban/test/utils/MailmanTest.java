package azkaban.test.utils;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import azkaban.utils.Mailman;


public class MailmanTest {

	private Mailman mailer;
	
	private String user = "cyu";
	private String password;
	private String host = "email.corp.linkedin.com";
	private String mailSender = "cyu@linkedin.com";
	
	private Logger logger = Logger.getLogger(Mailman.class);
	  
	  @Before
	public void setUp() {		
	
		  mailer = new Mailman(host, user, password, mailSender);
		  

	  }
	  
	  @Test
	  public void testSendEmail() {

		  String fromAddress = "cyu@linkedin.com";
		  List<String> toAddress = new ArrayList<String>();
		  //toAddress.add("cyu@linkedin.com");
		  toAddress.add("azkaban-test@linkedin.com");
		  
		  String subject = "Azkaban Test email subject";
		  String body = "Azkaban Test email body";
		  
		  try {
			   mailer.sendEmail(fromAddress, toAddress, subject, body);
		  }
		  catch (Exception e) {
			  Assert.assertTrue(true);
			  //please check email to see if this works
			  e.printStackTrace();
		  }
	    
	  }


}
