package ch.epfl.data.squall;

import java.io.IOException;

public class ExampleClient {

  public static void main(String[] args) {

    SquallClient sc;
    try {
      sc = new SquallClient("localhost", 9898);
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }
    sc.start();
    long start = System.currentTimeMillis();
    while(System.currentTimeMillis()-start < 10000) {
      try {
          Thread.sleep(1000);
      } catch(InterruptedException ex) {
          Thread.currentThread().interrupt();
      }

      System.out.println(sc.getNextTuple());
    }
    sc.close();
  }

}
