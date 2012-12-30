package api.better;

import java.io.IOException;
import java.net.Socket;
import java.util.*;

public class ConnectionManager {

  private static volatile ConnectionManager cm = null;

  private Set<Connection> connections;

  private ConnectionManager() {
    connections = Collections.synchronizedSet(new HashSet<Connection>());
  }

  public static ConnectionManager get() {
    if(cm == null) {
      synchronized (ConnectionManager.class) {
        if(cm == null) cm = new ConnectionManager();
      }
    }
    return cm;
  }

  public void add(Socket socket) throws IOException {

    Scanner in = new Scanner(socket.getInputStream());
    String protocol;
    try {
      protocol = in.nextLine();
    } catch (NoSuchElementException e) {
      // default
      protocol = "TCP";
    }
    if("TCP".equalsIgnoreCase(protocol)) {
      Connection conn = new TcpConnection(socket);
      this.connections.add(conn);
      conn.start();
    } else if("UDP".equalsIgnoreCase(protocol)) {
      throw new RuntimeException("Unimplemented protocol:" + protocol.toUpperCase());
    } else throw new RuntimeException("Unexpected protocol: " + protocol.toUpperCase());
  }

  public void remove(Connection conn) {
    this.connections.remove(conn);
  }

  public void send(final String tuple) {
    System.out.println("Sending: " + tuple);
    new Thread(new Runnable() {
      @Override
      public void run() {
        for(Connection conn : connections) {
          conn.add(tuple);
        }
      }
    }).start();
  }

  public void closeConnections() {
    new Thread(new Runnable() {
      @Override
      public void run() {
        for(Connection conn : connections) {
          conn.close();
        }
      }
    }).start();
  }
}
