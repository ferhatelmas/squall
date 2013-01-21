package api;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Collector extends Thread {

  private final static Logger LOGGER = Logger.getLogger(Collector.class.getName());

  private ServerSocket socket;
  private Set<CollectorWorker> workers;
  private volatile Status status;

  private Set<String> topologies;
  private TopologyChecker topologyChecker;
  private int period;

  public Collector(int port, int period) throws IOException {
    this.socket = new ServerSocket(port);
    this.workers = Collections.synchronizedSet(new HashSet<CollectorWorker>());
    this.topologies = Collections.synchronizedSet(new HashSet<String>());
    this.period = period;
  }

  @Override
  public void run() {
    status = Status.RUNNING;
    topologyChecker = new TopologyChecker(this, period);
    LOGGER.log(Level.INFO, "Topology Checker is started with period of " + period);

    while(status == Status.RUNNING) {
      try {
        CollectorWorker cw = new CollectorWorker(this, socket.accept());
        addWorker(cw);
        cw.start();
      } catch(SocketException e) {
        close();
      } catch(IOException e) {
        LOGGER.log(Level.WARNING, "New connection from bolts couldn't be handled");
      }
    }
  }

  public void addWorker(CollectorWorker cw) {
    this.workers.add(cw);
  }

  private void closeWorkers() {
    for(CollectorWorker cw : workers)
      cw.close();
    workers.clear();
  }

  public void addTopology(String topologyName) {
    this.topologies.add(topologyName);
  }

  public Set<String> getTopologies() {
    return this.topologies;
  }

  public void removeTopology(String topologyName) {
    // remove topology
    topologies.remove(topologyName);

    // get workers working on killed topology
    HashSet<CollectorWorker> toBeClosedWorkers = new HashSet<CollectorWorker>();
    for(CollectorWorker cw : workers) {
      if(cw.getTopologyName().equals(topologyName))
        toBeClosedWorkers.add(cw);
    }

    // close their connections
    for(CollectorWorker cw : toBeClosedWorkers) {
      cw.close();
    }

    // remove them from workers, too
    workers.removeAll(toBeClosedWorkers);
  }

  public void close() {
    if(status == Status.RUNNING) closeImpl();
  }

  private void closeImpl() {
    topologyChecker.cancel();
    topologies.clear();
    LOGGER.log(Level.INFO, "Topology Checker is closed");
    closeWorkers();
    LOGGER.log(Level.INFO, "Workers are closed");
    status = Status.CLOSED;
    try {
      if(socket != null && !socket.isClosed()) {
        LOGGER.log(Level.INFO, "Collector server is closing");
        socket.close();
      }
    } catch(IOException e) {
      LOGGER.log(Level.WARNING, "Collector server couldn't be closed properly");
    }
  }

}
