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
      sc = new SquallClient("localhost", 9898, srh);
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }
    sc.start();

    while(!sc.isClosed()) {
      // do other complex work
    }

    sc.close();
  }

}

