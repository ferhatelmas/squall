package api;

import java.io.IOException;
import java.util.Random;

public class ExampleServer {

  public static void main(String[] args) {
    Server server = null;
    try {
      server = new Server(9898);
      server.start();
    } catch (IOException e) {
      e.printStackTrace();
      if(server != null) server.close();
      return;
    }

    String[] tuples = new String[]{"ferhat", "koch", "avitorovic"};
    Random r = new Random(123457);

    int i = 0;
    while(i++ < 100) {
      ConnectionManager.get().send(tuples[r.nextInt(tuples.length)]);
      try {
        Thread.sleep(1000);
      } catch(InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }

    server.close();
    ConnectionManager.get().closeConnections();
  }

}
