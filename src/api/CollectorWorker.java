package api;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.SocketException;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CollectorWorker extends Thread {

  private final static Logger LOGGER = Logger.getLogger(CollectorWorker.class.getName());

  private Collector parent;
  private Socket socket;
  private BufferedReader br;

  private volatile Status status;

  private String topologyName;

  public CollectorWorker(Collector parent, Socket socket) throws IOException {
    System.out.println("New bolt connection: " + socket.getRemoteSocketAddress());
    this.parent = parent;
    this.socket = socket;
    this.br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
  }

  @Override
  public void run() {
    this.status = Status.RUNNING;
    boolean init = true;
    while(status == Status.RUNNING) {
      String tuple = read();
      if(tuple != null && !tuple.isEmpty()) {
        // Blade master can freeze
        // System.out.println("Got: " + tuple + " from " + socket.getRemoteSocketAddress());
        if(init) {
          topologyName = tuple;
          parent.addTopology(topologyName);
          init = false;
        }
        ConnectionManager.get().send(tuple);
      }
    }
  }

  private String read() {
    try {
      return br.ready() ? br.readLine() : null;
    } catch(IOException e) {
      return null;
    }
  }

  public void close() {
    status = Status.CLOSED;
    try {
      if(socket != null && !socket.isClosed()) {
        LOGGER.log(Level.INFO, "Bolt " + socket.getRemoteSocketAddress() + " is exiting");
        socket.close();
      }
    } catch(IOException e) {
      LOGGER.log(Level.WARNING, "Connection couldn't be closed properly: " +
          socket.getRemoteSocketAddress());
    }
  }

  public String getTopologyName() {
    return this.topologyName;
  }
}
