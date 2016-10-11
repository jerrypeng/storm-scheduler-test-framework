package org.apache.storm.scheduler.performance;

import org.apache.storm.Config;
import org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.optimal.OptimalStrategyConfigs;
import org.apache.storm.utils.Utils;
import org.apache.storm.scheduler.performance.runners.DataSetPerformanceRunner;
import org.apache.storm.scheduler.resource.strategies.scheduling.RoundRobinStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.nextgen.NextGenStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.nextgen.NextGenStrategy2;
import org.apache.storm.scheduler.resource.strategies.scheduling.nextgen.NextGenStrategy3;
import org.apache.storm.scheduler.resource.strategies.scheduling.nextgen.NextGenStrategy4;
import org.apache.storm.scheduler.resource.strategies.scheduling.nextgen.NextGenStrategy5;
import org.json.simple.parser.ParseException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by jerrypeng on 10/10/16.
 */
public class TestSchedulingStrategyPerformanceDataset {


    private static final Logger LOG = LoggerFactory.getLogger(TestSchedulingStrategyPerformanceDataset.class);
    private static final int NUM_SUPS = 20;
    private static final int NUM_WORKERS_PER_SUP = 4;
    private static final int MAX_TRAVERSAL_DEPTH = 5000000;
    private static final int NUM_WORKERS = NUM_SUPS * NUM_WORKERS_PER_SUP;
    private final String TOPOLOGY_SUBMITTER = "jerry";

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

    @Test
    public void runDataSet() throws ParseException, IOException, ClassNotFoundException {
        final Config defaultConfs = new Config();
        defaultConfs.putAll(getDefaultConfigs());

        final int THREAD_COUNT=8;
        final int ITERATIONS = 32 * 4;

        DataSetPerformanceRunner runner = new DataSetPerformanceRunner(THREAD_COUNT, ITERATIONS, defaultConfs, "notebook/rawResults");
        runner.addStormTopologyDataDir("dataset/stormTopologies");
        runner.addTopologyConfDataDir("dataset/topologyConfs");

        runner.addStrategy(RoundRobinStrategy.class);
        runner.addStrategy(DefaultResourceAwareStrategy.class);
        runner.addStrategy(NextGenStrategy.class);
        runner.addStrategy(NextGenStrategy2.class);
        runner.addStrategy(NextGenStrategy3.class);
        runner.addStrategy(NextGenStrategy4.class);
        runner.addStrategy(NextGenStrategy5.class);

        runner.run();
        runner.createTestReport("notebook/results.json");
    }
}
