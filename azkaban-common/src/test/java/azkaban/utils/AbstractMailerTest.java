package azkaban.utils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wqs on 17/1/9.
 */
public class AbstractMailerTest {

    @Test
    public void testCreateEmailMessage(){

        Props props = new Props();
        props.put("mail.port",445);
        props.put("mail.user","somebody");
        props.put("mail.password","pwd");
        props.put("mail.sender","somebody@xxx.com");
        props.put("server.port","114");
        props.put("server.useSSL","false");
        AbstractMailer mailer = new AbstractMailer(props);
        List<String> senderList = new ArrayList<String>();
        senderList.add("sender@xxx.com");
        EmailMessage emailMessage = mailer.createEmailMessage("subject","text/html",senderList);

        assert emailMessage.getMailPort()==445;
    }
}
