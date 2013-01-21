package ch.epfl.data.squall;

import java.io.IOException;

public class ExampleClient2 {

  public static void main(String[] args) {

    SquallClientHandler srh = new SquallClientHandler() {
      @Override
      public void newTupleArrived(String tuple) {
        System.out.println(tuple);
      }
    };

    SquallClient sc;
    try {
      sc = new SquallClient("icdatasrv2.epfl.ch", 9898, srh);
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }
    sc.start();

    while(!sc.isClosed()) {
      // do other complex work
      try {
        Thread.sleep(1000);
      } catch(InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

  }

}
