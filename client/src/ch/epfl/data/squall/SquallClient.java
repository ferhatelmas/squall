package ch.epfl.data.squall;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SquallClient extends Thread {

  private final static Logger LOGGER = Logger.getLogger(SquallClient.class.getName());

  public enum Status {CLOSED, RUNNING}

  private SquallClientHandler handler;

  private ConcurrentLinkedQueue<String> store;

  private Socket socket;
  private BufferedReader in;
  private PrintWriter out;
  private volatile Status status;

  public SquallClient(String host, int port) throws IOException {
    init(host, port, null);
  }

  public SquallClient(String host, int port, SquallClientHandler handler) throws IOException {
    init(host, port, handler);
  }

  private void init(String host, int port, SquallClientHandler handler) throws IOException {
    this.store = new ConcurrentLinkedQueue<String>();
    this.socket = new Socket(InetAddress.getByName(host), port);
    this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    this.out = new PrintWriter(socket.getOutputStream(), true);
    this.status = Status.RUNNING;
    this.handler = handler;
  }

  @Override
  public void run() {
    try {
      // configure server how to send tuples
      sendConfiguration();
      // heartbeat, check if server is alive before blocking read
      startHeartbeat();
      // read tuples, call handler
      processTuples();
    } finally {
      closeImpl();
    }
  }

  private void sendConfiguration() {
    // say how you want to get tuples
    // protocol to be decided

    // now only send TCP for connection type
    out.println("TCP");
    if(out.checkError()) status = Status.CLOSED;
  }

  private void startHeartbeat() {
    new Thread(new Runnable() {
      @Override
      public void run() {
        while(!out.checkError() && status == Status.RUNNING) {
          out.println("x");
          try {
            Thread.sleep(10);
          } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        LOGGER.log(Level.INFO, "Server is down or socket is explicitly closed, heartbeat stopping");
        closeImpl();
      }
    }).start();
  }

  private void processTuples() {
    while(true) {
      if(status == Status.CLOSED) {
        break;
      }

      String data = read();
      if(data != null && !data.isEmpty()) {
        if(("QUIT").equalsIgnoreCase(data)) {
          LOGGER.info("Got QUIT message from server");
          break;
        }
        else {
          if(handler == null) store.add(data);
          else {
            final String finalData = data;
            new Thread(new Runnable() {
              @Override
              public void run() {
                handler.newTupleArrived(finalData);
              }
            }).start();
          }
        }
      }
    }
  }

  // read is blocking due to nature of skipping input on stream
  // however another thread checks if server crashed
  private String read() {
    try {
      return in.ready() ? in.readLine() : null;
    } catch(IOException e) {
      return null;
    }
  }

  private synchronized void closeImpl() {
    try {
      // No need to close streams, already closed when socket is closed
      // if(out != null) out.close();
      // if(in != null) in.close();
      status = Status.CLOSED;
      if(socket != null && !socket.isClosed()) socket.close();
    } catch (IOException e) {
      LOGGER.log(Level.WARNING, "Connection to cluster couldn't closed properly", e);
    }
  }

  public String awaitNextTuple() {
    String data = store.poll();
    while(data == null && status == Status.RUNNING) data = store.poll();
    return data;
  }

  public String getNextTuple() {
    return store.poll();
  }

  public synchronized void close() {
    this.status = Status.CLOSED;
  }

  public boolean isClosed() {
    return status == Status.CLOSED;
  }
}
