package api.better;

import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class Connection extends Thread {

  protected enum Status {CLOSED, RUNNING}

  protected volatile Status status;

  protected ConcurrentLinkedQueue<String> stream = new ConcurrentLinkedQueue<String>();;

  public void add(String tuple) {
    this.stream.add(tuple);
  }

  public void close() {
    this.status = Status.CLOSED;
  }

}
