package azkaban.execapp.fake;

import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;

public class FakeServer extends Server {

    @Override
    public Connector[] getConnectors() {
        return new Connector[] {new SocketConnector()};
    }

}
