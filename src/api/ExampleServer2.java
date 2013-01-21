package api;

import java.io.IOException;

public class ExampleServer2 {

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
    System.out.println("Server is started at port: " + 9898);

    Collector collector = null;
    try {
      collector = new Collector(10061, 1000);
      collector.start();
    } catch (IOException e) {
      e.printStackTrace();
      if(collector != null) collector.close();
      return;
    }
    System.out.println("Collector is started at port: " + 10061);

    try {
      Thread.sleep(1000000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    collector.close();
    server.close();
    ConnectionManager.get().closeConnections();
  }

}
