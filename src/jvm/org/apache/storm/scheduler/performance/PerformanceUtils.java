package org.apache.storm.scheduler.performance;

import org.apache.storm.Config;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jerrypeng on 8/11/16.
 */
public class PerformanceUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PerformanceUtils.class);

    private static int currentTime = 1450418597;

    public static List<TopologyDetails> getListOfTopologies(Config config) {

        List<TopologyDetails> topos = new LinkedList<TopologyDetails>();

        topos.add(PerformanceUtils.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, 20));
        topos.add(PerformanceUtils.getTopology("topo-2", config, 5, 15, 1, 1, currentTime - 8, 30));
        topos.add(PerformanceUtils.getTopology("topo-3", config, 5, 15, 1, 1, currentTime - 16, 30));
        topos.add(PerformanceUtils.getTopology("topo-4", config, 5, 15, 1, 1, currentTime - 16, 20));
        topos.add(PerformanceUtils.getTopology("topo-5", config, 5, 15, 1, 1, currentTime - 24, 30));
        topos.add(PerformanceUtils.getTopology("topo-6", config, 5, 15, 1, 1, currentTime - 2, 0));
        topos.add(PerformanceUtils.getTopology("topo-7", config, 5, 15, 1, 1, currentTime - 8, 0));
        topos.add(PerformanceUtils.getTopology("topo-8", config, 5, 15, 1, 1, currentTime - 16, 15));
        topos.add(PerformanceUtils.getTopology("topo-9", config, 5, 15, 1, 1, currentTime - 16, 8));
        topos.add(PerformanceUtils.getTopology("topo-10", config, 5, 15, 1, 1, currentTime - 24, 9));
        return topos;
    }

    public static List<String> getListOfTopologiesCorrectOrder() {
        List<String> topos = new LinkedList<String>();
        topos.add("topo-7");
        topos.add("topo-6");
        topos.add("topo-9");
        topos.add("topo-10");
        topos.add("topo-8");
        topos.add("topo-4");
        topos.add("topo-1");
        topos.add("topo-5");
        topos.add("topo-3");
        topos.add("topo-2");
        return topos;
    }

    public static Map<String, SupervisorDetails> genSupervisors(int numSup, int numPorts, Map resourceMap) {
        return genSupervisors(numSup, numPorts, 0, resourceMap);
    }

    public static Map<String, SupervisorDetails> genSupervisors(int numSup, int numPorts, int start, Map resourceMap) {
        Map<String, SupervisorDetails> retList = new HashMap<String, SupervisorDetails>();
        for (int i = start; i < numSup + start; i++) {
            List<Number> ports = new LinkedList<Number>();
            for (int j = 0; j < numPorts; j++) {
                ports.add(j);
            }
            SupervisorDetails sup = new SupervisorDetails("sup-" + i, "host-" + i, null, ports, new HashMap<>(resourceMap));
            retList.put(sup.getId(), sup);
        }
        return retList;
    }

    public static Map<ExecutorDetails, String> genExecsAndComps(StormTopology topology) {
        Map<ExecutorDetails, String> retMap = new HashMap<ExecutorDetails, String>();
        int startTask = 0;
        int endTask = 1;
        for (Map.Entry<String, SpoutSpec> entry : topology.get_spouts().entrySet()) {
            SpoutSpec spout = entry.getValue();
            String spoutId = entry.getKey();
            int spoutParallelism = spout.get_common().get_parallelism_hint();
            if (spoutParallelism <= 0) {
                try {
                    JSONObject componentConf = ((JSONObject) new JSONParser().parse(spout.get_common().get_json_conf()));
                    if (componentConf != null && componentConf.containsKey(Config.TOPOLOGY_TASKS)) {
                        int numTasks = Utils.getInt(componentConf.get(Config.TOPOLOGY_TASKS));
                        spoutParallelism = numTasks;
                    }
                } catch (ParseException ex) {
                    throw new RuntimeException(ex);
                }
            }

            for (int i = 0; i < spoutParallelism; i++) {
                retMap.put(new ExecutorDetails(startTask, endTask), spoutId);
                startTask++;
                endTask++;
            }
        }

        for (Map.Entry<String, Bolt> entry : topology.get_bolts().entrySet()) {
            String boltId = entry.getKey();
            Bolt bolt = entry.getValue();
            int boltParallelism = bolt.get_common().get_parallelism_hint();
            if (boltParallelism <= 0) {
                try {
                    JSONObject componentConf = ((JSONObject) new JSONParser().parse(bolt.get_common().get_json_conf()));
                    if (componentConf != null && componentConf.containsKey(Config.TOPOLOGY_TASKS)) {
                        int numTasks = Utils.getInt(componentConf.get(Config.TOPOLOGY_TASKS));
                        boltParallelism = numTasks;
                    }
                } catch (ParseException ex) {
                    throw new RuntimeException(ex);
                }
            }

            for (int i = 0; i < boltParallelism; i++) {
                retMap.put(new ExecutorDetails(startTask, endTask), boltId);
                startTask++;
                endTask++;
            }
        }
        return retMap;
    }

    public static TopologyDetails getTopology(String name, Map config, int numSpout, int numBolt,
                                              int spoutParallelism, int boltParallelism, int launchTime, int priority) {

        Config conf = new Config();
        conf.putAll(config);
        conf.put(Config.TOPOLOGY_PRIORITY, priority);
        conf.put(Config.TOPOLOGY_NAME, name);
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, Double.MAX_VALUE);
        StormTopology topology = buildTopology(numSpout, numBolt, spoutParallelism, boltParallelism);
        TopologyDetails topo = new TopologyDetails(name + "-" + launchTime, conf, topology,
                0,
                genExecsAndComps(topology), launchTime);
        return topo;
    }

    public static StormTopology buildTopology(int numSpout, int numBolt,
                                              int spoutParallelism, int boltParallelism) {
        LOG.debug("buildTopology with -> numSpout: " + numSpout + " spoutParallelism: "
                + spoutParallelism + " numBolt: "
                + numBolt + " boltParallelism: " + boltParallelism);
        TopologyBuilder builder = new TopologyBuilder();

        for (int i = 0; i < numSpout; i++) {
            SpoutDeclarer s1 = builder.setSpout("spout-" + i, new TestSpout(),
                    spoutParallelism);
        }
        int j = 0;
        for (int i = 0; i < numBolt; i++) {
            if (j >= numSpout) {
                j = 0;
            }
            BoltDeclarer b1 = builder.setBolt("bolt-" + i, new TestBolt(),
                    boltParallelism).shuffleGrouping("spout-" + j);
            j++;
        }

        return builder.createTopology();
    }

    public static class TestSpout extends BaseRichSpout {
        boolean _isDistributed;
        SpoutOutputCollector _collector;

        public TestSpout() {
            this(true);
        }

        public TestSpout(boolean isDistributed) {
            _isDistributed = isDistributed;
        }

        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
        }

        public void close() {
        }

        public void nextTuple() {
            Utils.sleep(100);
            final String[] words = new String[]{"nathan", "mike", "jackson", "golda", "bertels"};
            final Random rand = new Random();
            final String word = words[rand.nextInt(words.length)];
            _collector.emit(new Values(word));
        }

        public void ack(Object msgId) {
        }

        public void fail(Object msgId) {
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            if (!_isDistributed) {
                Map<String, Object> ret = new HashMap<String, Object>();
                ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
                return ret;
            } else {
                return null;
            }
        }
    }

    public static class TestBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context,
                            OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class INimbusTest implements INimbus {
        @Override
        public void prepare(Map stormConf, String schedulerLocalDir) {

        }

        @Override
        public Collection<WorkerSlot> allSlotsAvailableForScheduling(Collection<SupervisorDetails> existingSupervisors, Topologies topologies, Set<String> topologiesMissingAssignments) {
            return null;
        }

        @Override
        public void assignSlots(Topologies topologies, Map<String, Collection<WorkerSlot>> newSlotsByTopologyId) {

        }

        @Override
        public String getHostName(Map<String, SupervisorDetails> existingSupervisors, String nodeId) {
            if (existingSupervisors.containsKey(nodeId)) {
                return existingSupervisors.get(nodeId).getHost();
            }
            return null;
        }

        @Override
        public IScheduler getForcedScheduler() {
            return null;
        }
    }

    private static boolean isContain(String source, String subItem) {
        String pattern = "\\b" + subItem + "\\b";
        Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(source);
        return m.find();
    }

    public static boolean assertStatusSuccess(String status) {
        return isContain(status, "fully") && isContain(status, "scheduled") && !isContain(status, "unsuccessful");
    }

    public static TopologyDetails findTopologyInSetFromName(String topoName, Set<TopologyDetails> set) {
        TopologyDetails ret = null;
        for (TopologyDetails entry : set) {
            if (entry.getName().equals(topoName)) {
                ret = entry;
            }
        }
        return ret;
    }

    public static Map<ExecutorDetails, WorkerSlot> getExecToWorkerResultMapping(Map<WorkerSlot, Collection<ExecutorDetails>> workerToExecs) {
        Map<ExecutorDetails, WorkerSlot> execToWorkerMapping = null;
        if(workerToExecs!= null) {
            execToWorkerMapping = new HashMap<ExecutorDetails, WorkerSlot>();
            for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> entry : workerToExecs.entrySet()) {
                WorkerSlot workerSlot = entry.getKey();
                Collection<ExecutorDetails> execs = entry.getValue();
                for (ExecutorDetails exec : execs) {
                    execToWorkerMapping.put(exec, workerSlot);
                }
            }
        }
        return execToWorkerMapping;
    }

    public static List<Number> genPorts(int numPorts) {
        List<Number> ports = new LinkedList<Number>();
        for (int j = 0; j < numPorts; j++) {
            ports.add(j);
        }
        return ports;
    }

    public static int getRandomIntInRange(int min, int max, Random random) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        return random.nextInt((max - min)) + min;
    }

    public static class Triple<X,Y,Z> {
        public final X x;
        public final Y y;
        public final Z z;
        public Triple(X x, Y y, Z z) {
            this.x = x;
            this.y = y;
            this.z = z;
        }
    }

    public static Map<String, String> getSupIdToRack(Cluster cluster) {
        Map<String, String> supIdsToRack = new HashMap<String, String>();
        for (Map.Entry<String, List<String>> entry : cluster.getNetworkTopography().entrySet()) {
            String rackId = entry.getKey();
            List<String> hosts = entry.getValue();
            for (String host : hosts) {
                for (SupervisorDetails supervisorDetails : cluster.getSupervisorsByHost(host)) {
                    supIdsToRack.put(supervisorDetails.getId(), rackId);
                }
            }
        }
        return supIdsToRack;
    }
}
