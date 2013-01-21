package api;

import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;
import org.apache.thrift7.TException;

import java.lang.String;
import java.util.HashSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TopologyChecker extends Timer {

  private final static Logger LOGGER = Logger.getLogger(TopologyChecker.class.getName());

  private static final String NIMBUS_HOST = "icdatasrv5";
  private static final int NIMBUS_THRIFT_PORT = 6627;

  private TopologyCheckTask tct;

  public TopologyChecker(Collector collector, long period) {
    this.tct = new TopologyCheckTask(collector,
               new NimbusClient(NIMBUS_HOST, NIMBUS_THRIFT_PORT).getClient());

    scheduleAtFixedRate(tct, 0, period);
  }

  public final class TopologyCheckTask extends TimerTask {

    private Collector collector;
    private Client client;

    public TopologyCheckTask(Collector collector, Client client) {
      this.collector = collector;
      this.client = client;
    }

    public void run() {
      try {
        // collect alive topologies
        HashSet<String> aliveTopologyNames = new HashSet<String>();
        for(TopologySummary ts : client.getClusterInfo().get_topologies()) {
          aliveTopologyNames.add(ts.get_name());
        }

        // a trick to prevent ConcurrentModificationException
        // since we can't update collection while iterating it
        HashSet<String> killedTopologyNames = new HashSet<String>();
        for(String topologyName : collector.getTopologies()) {
          if(!aliveTopologyNames.contains(topologyName))
            killedTopologyNames.add(topologyName);
        }

        for(String killedTopologyName : killedTopologyNames)
          collector.removeTopology(killedTopologyName);
      } catch(TException e) {
        // Not much important, after period, it will be rechecked.
        LOGGER.log(Level.INFO, "Alive topologies couldn't be checked this time");
      }
    }

  }

}
