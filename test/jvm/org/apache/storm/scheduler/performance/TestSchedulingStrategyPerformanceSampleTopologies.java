package org.apache.storm.scheduler.performance;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.SchedulerAssignmentImpl;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.optimal.OptimalStrategyConfigs;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.scheduler.resource.strategies.scheduling.RoundRobinStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.nextgen.NextGenStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.nextgen.NextGenStrategy2;
import org.apache.storm.scheduler.resource.strategies.scheduling.nextgen.NextGenStrategy3;
import org.apache.storm.scheduler.resource.strategies.scheduling.optimal.OptimalStrategy;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jerrypeng on 8/2/16.
 */
public class TestSchedulingStrategyPerformanceSampleTopologies {

    private static final Logger LOG = LoggerFactory.getLogger(TestSchedulingStrategyPerformanceSampleTopologies.class);
    private static final int NUM_SUPS = 20;
    private static final int NUM_WORKERS_PER_SUP = 4;
    private static final int MAX_TRAVERSAL_DEPTH = 5000000;
    private static final int NUM_WORKERS = NUM_SUPS * NUM_WORKERS_PER_SUP;
    private final String TOPOLOGY_SUBMITTER = "jerry";

    public static Map getDefaultClusterConfigs() {
        Map config = new HashMap();
        config.put(Config.SUPERVISOR_CPU_CAPACITY, 400.0);
        config.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1024.0 * 4);
        return config;
    }

    public Map getDefaultConfigs() {
        Map config = Utils.readStormConfig();
        config.put(OptimalStrategyConfigs.TOPOLOGY_CONSTRAINTS_MAX_DEPTH_TRAVERSAL, MAX_TRAVERSAL_DEPTH);
        config.put(Config.TOPOLOGY_SUBMITTER_USER, TOPOLOGY_SUBMITTER);
        config.put(Config.TOPOLOGY_WORKERS, NUM_WORKERS);
        config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 100000);
        config.put(Config.TOPOLOGY_PRIORITY, 1);
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 100);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 100);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 0.0);
        return config;
    }

    public static Map<String, SupervisorDetails>  getSupervisors(Map config) {
        Map<String, SupervisorDetails> supMap = new HashMap<>();

        SupervisorDetails sup1 = new SupervisorDetails("sup-1", "host-1", null, PerformanceUtils.genPorts(1), new HashMap<>(config));
        supMap.put(sup1.getId(), sup1);

        config.put(Config.SUPERVISOR_CPU_CAPACITY, 200.0);
        config.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1024.0 * 4);
        SupervisorDetails sup2 = new SupervisorDetails("sup-2", "host-2", null, PerformanceUtils.genPorts(1), new HashMap<>(config));
        supMap.put(sup2.getId(), sup2);

        config.put(Config.SUPERVISOR_CPU_CAPACITY, 100.0);
        config.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1024.0 * 4);
        SupervisorDetails sup3 = new SupervisorDetails("sup-3", "host-3", null, PerformanceUtils.genPorts(1), new HashMap<>(config));
        supMap.put(sup3.getId(), sup3);

        config.put(Config.SUPERVISOR_CPU_CAPACITY, 700.0);
        config.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1024.0 * 4);
        SupervisorDetails sup4 = new SupervisorDetails("sup-4", "host-4", null, PerformanceUtils.genPorts(1), new HashMap<>(config));
        supMap.put(sup4.getId(), sup4);

        return supMap;
    }

    public static  Map<String, List<String>> getNetworkTopography() {
        Map<String, List<String>> ret = new HashMap<>();
        ret.put("rack-1", Arrays.asList("host-1", "host-2"));
        ret.put("rack-2", Arrays.asList("host-3", "host-4"));
        return ret;
    }

    @Test
    public void testSampleTopologies() {
        PerformanceTestSuite perf = new PerformanceTestSuite(getDefaultConfigs(), "notebook/rawResults/jsonLog");

        Cluster cluster = new Cluster(new PerformanceUtils.INimbusTest(), getSupervisors(getDefaultClusterConfigs()), new HashMap<String, SchedulerAssignmentImpl>(), getDefaultClusterConfigs());
        cluster.setNetworkTopography(getNetworkTopography());
        perf.buildTestCluster(cluster);

        //adding strategies to test
        perf.addSchedulingStrategyToTest(RoundRobinStrategy.class);
        perf.addSchedulingStrategyToTest(DefaultResourceAwareStrategy.class);
        perf.addSchedulingStrategyToTest(OptimalStrategy.class);
        perf.addSchedulingStrategyToTest(NextGenStrategy.class);
        perf.addSchedulingStrategyToTest(NextGenStrategy2.class);
        perf.addSchedulingStrategyToTest(NextGenStrategy3.class);
        //adding test topologies
        perf.addTestTopology(getTestTopology1());
        perf.addTestTopology(getTestTopology2());
        perf.addTestTopology(getTestTopology3());
        perf.run();
        perf.endTest();
        perf.createTestReport("notebook/results.json");
    }


    public TopologyDetails getTestTopology1() {
        return PerformanceUtils.getTopology("topo-1", getDefaultConfigs(), 2, 3, 1, 1, 0, 0);
    }

    public TopologyDetails getTestTopology2() {
        TopologyBuilder builder = new TopologyBuilder();

        SpoutDeclarer s1 = builder.setSpout("spout-1", new PerformanceUtils.TestSpout(), 1).setCPULoad(100.0).setMemoryLoad(1024.0);
        SpoutDeclarer s2 = builder.setSpout("spout-2", new PerformanceUtils.TestSpout(), 1).setCPULoad(150.0).setMemoryLoad(768.0);
        BoltDeclarer b1 = builder.setBolt("bolt-1", new PerformanceUtils.TestBolt(), 1).shuffleGrouping("spout-1").shuffleGrouping("bolt-3").setCPULoad(200).setMemoryLoad(2048.0);

        BoltDeclarer b2 = builder.setBolt("bolt-2", new PerformanceUtils.TestBolt(), 1).shuffleGrouping("bolt-1").setCPULoad(50.0).setMemoryLoad(512.0);
        BoltDeclarer b3 = builder.setBolt("bolt-3", new PerformanceUtils.TestBolt(), 1).shuffleGrouping("bolt-2").shuffleGrouping("spout-2").setCPULoad(100.0).setMemoryLoad(1024.0);

        StormTopology stormTopology = builder.createTopology();
        return new TopologyDetails("topo-2", getDefaultConfigs(), stormTopology, 0, PerformanceUtils.genExecsAndComps(stormTopology), 0);
    }

    public TopologyDetails getTestTopology3() {
        TopologyBuilder builder = new TopologyBuilder();

        SpoutDeclarer s1 = builder.setSpout("spout-1", new PerformanceUtils.TestSpout(), 4).setCPULoad(100.0).setMemoryLoad(64.0);
        BoltDeclarer b1 = builder.setBolt("bolt-1", new PerformanceUtils.TestBolt(), 4).shuffleGrouping("spout-1").setCPULoad(100.0).setMemoryLoad(64.0);
        BoltDeclarer b2 = builder.setBolt("bolt-2", new PerformanceUtils.TestBolt(), 2).shuffleGrouping("bolt-1").setCPULoad(100.0).setMemoryLoad(64.0);
        StormTopology stormTopology = builder.createTopology();
        return new TopologyDetails("topo-3", getDefaultConfigs(), stormTopology, 0, PerformanceUtils.genExecsAndComps(stormTopology), 0);
    }
}
