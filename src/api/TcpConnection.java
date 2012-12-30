package api.better;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TcpConnection extends Connection {

  private final static Logger LOGGER = Logger.getLogger(TcpConnection.class.getName());

  private Socket socket;
  private PrintWriter pw;

  public TcpConnection(Socket socket) throws IOException {
    this.socket = socket;
    this.pw = new PrintWriter(socket.getOutputStream());
  }

  @Override
  public void run() {
    status = Status.RUNNING;
    String tuple;
    while(true) {
      tuple = status == Status.RUNNING ? stream.poll() : "QUIT";
      if(tuple != null) {
        System.out.println("Got " + tuple + " from stream");
        pw.println(tuple);
        if(pw.checkError() || "QUIT".equals(tuple)) {
          // Remote end point is closed
          // Close socket and notify connection manager
          closeImpl();
          break;
        }
      }
    }
  }

  private void closeImpl() {
    try {
      socket.close();
      ConnectionManager.get().remove(this);
      LOGGER.log(Level.INFO, "Connection is closed: " + socket.getRemoteSocketAddress());
    } catch(IOException e) {
      LOGGER.log(Level.WARNING, "Connection couldn't be closed properly: " +
          socket.getRemoteSocketAddress(), e);
    }
  }

}