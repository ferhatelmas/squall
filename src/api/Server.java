package api.better;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Server extends Thread {

  private final static Logger LOGGER = Logger.getLogger(Server.class.getName());

  private ServerSocket serverSocket;

  public Server(int port) throws IOException {
    this.serverSocket = new ServerSocket(port);
  }

  @Override
  public void run() {
    while(true) {
      try {
        handleNewConnection(serverSocket.accept());
      } catch (SocketException e) {
        // It is thrown when server socket is closed
        // it will be closed by another thread
        // when stream is done, or explicitly closing taking more connections
        break;
      } catch(IOException e) {
        LOGGER.log(Level.WARNING, "New connection couldn't be handled", e);
      }
    }
  }

  private void handleNewConnection(final Socket newConn) {
    new Thread(new Runnable() {
      @Override
      public void run() {
        ExecutorService service = Executors.newSingleThreadExecutor();
        service.execute(new Runnable() {
          @Override
          public void run() {
            try {
              ConnectionManager.get().add(newConn);
            } catch(SocketException e) {
              // Later, if default is assumed this exception will never be thrown
              LOGGER.log(Level.WARNING, "Client doesn't follow the protocol: Nothing on the input channel" );
              closeNewConnection(newConn);
            } catch(IOException e) {
              LOGGER.log(Level.WARNING, "Connection with requested parameters couldn't be created: " +
                  newConn.getRemoteSocketAddress(), e);
              closeNewConnection(newConn);
            } catch(RuntimeException e) {
              LOGGER.log(Level.WARNING, "Client doesn't follow the protocol: " +
                  newConn.getRemoteSocketAddress(), e);
              closeNewConnection(newConn);
            }
          }
        });
        try {
          if(!service.awaitTermination(10, TimeUnit.SECONDS)) closeNewConnection(newConn);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }).start();
  }

  private void closeNewConnection(Socket socket) {
    try {
      socket.close();
    } catch(IOException e) {
      LOGGER.log(Level.WARNING, "New connection socket couldn't be closed: " +
          socket.getRemoteSocketAddress(), e);
    }
  }

  public void close() {
    try {
      if(serverSocket != null && !serverSocket.isClosed()) serverSocket.close();
      LOGGER.log(Level.INFO, "TCP Server is closed");
    } catch (IOException e) {
      LOGGER.log(Level.WARNING, "TCP Server couldn't be closed properly", e);
    }
  }
}