package api.better;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UdpConnection extends Connection {

  private final static Logger LOGGER = Logger.getLogger(UdpConnection.class.getName());

  private DatagramSocket socket;
  private SocketAddress remote;

  public UdpConnection(DatagramSocket socket, SocketAddress remote) {
    this.socket = socket;
    this.remote = remote;
  }

  @Override
  public void run() {
    String tuple;
    while(true) {
      tuple = status == Status.RUNNING ? stream.poll() : "QUIT";
      byte[] buf = (tuple + "\n").getBytes();
      try {
        socket.send(new DatagramPacket(buf, buf.length, remote));
      } catch(SocketException e) {
        LOGGER.log(Level.WARNING, "Tuple couldn't be send to: " + socket.getRemoteSocketAddress());
      } catch(IOException e) {
        // ...
      }
    }
  }

}
