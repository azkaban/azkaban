package azkaban.utils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wqs on 17/1/11.
 */
public class EmailerTest {


    /**
     * test emailMessage properties
     */
    @Test
    public void testCreateEmailMessage(){
        List<String> senderList = new ArrayList<String>();
        senderList.add("sender@xxx.com");
        EmailMessage emailMessage = null;


        //for Custom
        Props props = new Props();
        props.put("mail.port",445);
        props.put("mail.user","somebody");
        props.put("mail.password","pwd");
        props.put("mail.sender","somebody@xxx.com");
        props.put("server.port","114");
        props.put("jetty.use.ssl","false");
        props.put("server.useSSL","false");
        props.put("jetty.port","8786");
        AbstractMailer mailer = new Emailer(props);
        emailMessage = mailer.createEmailMessage("subject","text/html",senderList);

        assert emailMessage.getMailPort()==445;


        //for default
        Props defaultProps = new Props();
        defaultProps.put("mail.user","somebody");
        defaultProps.put("mail.password","pwd");
        defaultProps.put("mail.sender","somebody@xxx.com");
        defaultProps.put("server.port","114");
        defaultProps.put("server.useSSL","false");
        defaultProps.put("server.useSSL","true");
        defaultProps.put("jetty.ssl.port","8080");
        mailer = new Emailer(defaultProps);
        emailMessage = mailer.createEmailMessage("subject","text/html",senderList);

        assert emailMessage.getMailPort()==25;

    }

}
