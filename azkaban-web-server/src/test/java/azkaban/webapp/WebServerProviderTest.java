package azkaban.webapp;

import azkaban.utils.Props;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Response;
import org.mortbay.jetty.Server;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.mortbay.jetty.Handler.REQUEST;

public class WebServerProviderTest {

    private static final Props props = new Props();
    private static Server server;

    @BeforeClass
    public static void setUp() throws Exception {
        props.put("server.port", "0");
        props.put("jetty.port", "0");
        props.put("server.useSSL", "true");
        props.put("jetty.use.ssl", "false");
        props.put("jetty.disable.http-methods", "TRACE");

        AbstractModule propsInjector = new AbstractModule() {
            @Override
            protected void configure() {
                bind(Props.class).toInstance(props);
            }
        };

        WebServerProvider provider = Guice.createInjector(
                propsInjector
        ).getInstance(WebServerProvider.class);
        server = provider.get();
        server.start();

    }

    @AfterClass
    public static void tearDown() throws Exception {
        server.stop();
    }

    @Test
    public void whenTraceIsDisabledThen403Return() throws Exception {
        HttpServletRequest requestMock = Mockito.mock(Request.class);
        Mockito.when(requestMock.getMethod()).thenReturn("TRACE");
        HttpServletResponse responseMock = Mockito.mock(Response.class);

        server.handle("/some", requestMock, responseMock, REQUEST);
        Mockito.verify(responseMock, Mockito.atLeastOnce()).sendError(403);
    }

}