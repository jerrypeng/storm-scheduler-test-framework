package org.apache.storm.scheduler.performance;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.SchedulingResult;
import org.apache.storm.scheduler.resource.SchedulingState;
import org.apache.storm.scheduler.resource.User;
import org.apache.storm.scheduler.resource.strategies.scheduling.IStrategy;

import org.apache.storm.scheduler.resource.ResourceExtraUtils;
import org.apache.storm.scheduler.performance.metrics.ClusterResourceUtil;
import org.apache.storm.scheduler.performance.metrics.SchedulingNetworkMetric;

import org.json.simple.JSONObject;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jerrypeng on 8/1/16.
 */
public class PerformanceTestSession {
    private long testSessionId;

    private List<Class<? extends IStrategy>> strategies;

    private Cluster cluster;

    private Topologies topologies;

    private Map defaultConfigs = new HashMap();

    private Map<Class<? extends IStrategy>, PerformanceResult> performanceResults = new ConcurrentHashMap<>();

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(PerformanceTestSession.class);

    public static final class Config {
        public static final String testSessionId = "testSessionId";
        public static final String performanceResults = "performanceResults";
    }

    public PerformanceTestSession(Cluster cluster, Topologies topologies, List<Class<? extends IStrategy>> strategies, Map defaultConfigs, long sessionId) {
        this.testSessionId = sessionId;
        this.defaultConfigs.putAll(defaultConfigs);
        this.cluster = cluster;
        this.topologies = topologies;
        this.strategies = strategies;
        for (Class<? extends IStrategy> strategy : strategies) {
            this.performanceResults.put(strategy, new PerformanceResult());
        }
    }

    public Map<Class<? extends IStrategy>, PerformanceResult> getPerformanceResults() {
        return this.performanceResults;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("\n\n /******** STATS *********\n\n");

        for (Map.Entry<Class<? extends IStrategy>, PerformanceResult> entry : this.performanceResults.entrySet()) {
            sb.append("!------ ").append(entry.getKey().getCanonicalName()).append(" ------!\n");
            for (PerTopologyPerformanceResult perTopologyPerformanceResult : entry.getValue().getPerTopologyPerformanceResultMap().values()) {
                sb.append("!-- ").append(perTopologyPerformanceResult.getTopoId()).append(" --!\n");
                sb.append(perTopologyPerformanceResult + "\n\n");
            }
            sb.append(entry.getValue() + "\n\n");
        }
        return sb.toString();
    }

    public JSONObject toJsonObject() {
        JSONObject object = new JSONObject();
        object.put(Config.testSessionId, this.testSessionId);

        JSONObject tmpObject = new JSONObject();
        for (Map.Entry<Class<? extends IStrategy>, PerformanceResult> entry : this.performanceResults.entrySet()) {
            String clazz = entry.getKey().getSimpleName();
            PerformanceResult performanceResult = entry.getValue();
            tmpObject.put(clazz, performanceResult.toJsonObject());
        }
        object.put(Config.performanceResults, tmpObject);
        return object;
    }

    private SchedulingResult runStrategy(Class<? extends IStrategy> clazz, TopologyDetails topo, SchedulingState state) throws IllegalAccessException, InstantiationException {
        SchedulingResult result = null;
        try {
            IStrategy strategy = clazz.newInstance();
            LOG.info("<!--- Start scheduling topo {} with {} ---!>", topo.getId(), clazz.getCanonicalName());
            //get start time to profile runtime
            long startTime = System.nanoTime();
            strategy.prepare(new SchedulingState(new HashMap<String, User>(), state.cluster, state.topologies, state.conf));
            result = strategy.schedule(topo);
            LOG.info("result: {}", result);
            //get end time
            long endTime = System.nanoTime();
            LOG.info("<!--- end scheduling topo {} with {} ---!>", topo.getId(), clazz.getCanonicalName());
            //calculate runtime
            long duration = (endTime - startTime);
            //realize scheduling
            mkAssignments(state, result, topo);
            getPerToplogySchedulingStats(state, topo, clazz, result, duration);
        } catch (Exception ex) {
            LOG.info("Exception:", ex);
            LOG.info("Sups: {}", state.cluster.getSupervisors());
            LOG.info("topo: {}", topo);
            LOG.info("comps: {} execs: {}", topo.getComponents(), topo.getExecutors());
            LOG.info("{}",  ResourceExtraUtils.printScheduling(state.cluster, state.topologies));
            throw new RuntimeException(ex);
        }

        return result;
    }

    public void run() throws InstantiationException, IllegalAccessException {

        for (Class<? extends IStrategy> strategyClass : this.strategies) {
            //create new scheduling tmp state for each strategy
            SchedulingState tmpState = new SchedulingState(new HashMap<String, User>(), this.cluster, this.topologies, this.defaultConfigs);
            for (TopologyDetails topo : this.topologies.getTopologies()) {
                SchedulingResult result = runStrategy(strategyClass, topo, tmpState);
            }
            getOverallSchedulingUtilStats(strategyClass, tmpState);
        }
    }

    private void mkAssignments(SchedulingState state, SchedulingResult result, TopologyDetails topo) {
        if (result.isSuccess()) {
            for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> entry : result.getSchedulingResultMap().entrySet()) {
                WorkerSlot ws = entry.getKey();
                Collection<ExecutorDetails> execs = entry.getValue();
                state.cluster.assign(ws, topo.getId(), execs);
            }
        }
    }

    private void getPerToplogySchedulingStats(SchedulingState state, TopologyDetails topo, Class<? extends IStrategy> strategyClass, SchedulingResult result, long duration) {
        PerTopologyPerformanceResult perTopologyPerformanceResult = new PerTopologyPerformanceResult();
        perTopologyPerformanceResult.setTimeToRun(duration);
        if (result.isSuccess()) {
            //make sure strategy actually assigned everything
            assert(state.cluster.getUnassignedExecutors(topo).size() == 0);
            SchedulingNetworkMetric schedulingNetworkMetric = new SchedulingNetworkMetric(state.cluster, topo);
            perTopologyPerformanceResult.setNetworkClosenessMetric(schedulingNetworkMetric.getNetworkClosenessMetric());
            perTopologyPerformanceResult.setPrintableStats(schedulingNetworkMetric.toString());
            LOG.info(schedulingNetworkMetric.toString());
        }

        perTopologyPerformanceResult.setResult(result);
        perTopologyPerformanceResult.setTopoId(topo.getId());

        Map<String, PerTopologyPerformanceResult> perTopologyPerformanceResultMap = this.performanceResults.get(strategyClass).getPerTopologyPerformanceResultMap();
        perTopologyPerformanceResultMap.put(topo.getId(), perTopologyPerformanceResult);
    }

    private void getOverallSchedulingUtilStats(Class<? extends IStrategy> strategyClass, SchedulingState state) {
        this.getClusterUtilStats(strategyClass, state);
    }

    private void getClusterUtilStats(Class<? extends IStrategy> strategyClass, SchedulingState state) {
        ClusterResourceUtil clusterResourceUtil = new ClusterResourceUtil(state.cluster, state.topologies);
        PerformanceResult performanceResult = this.performanceResults.get(strategyClass);
        performanceResult.setAvgCpuUtilization(clusterResourceUtil.getAvgCpuUtil());
        performanceResult.setAvgMemoryUtilization(clusterResourceUtil.getAvgMemUtil());
        performanceResult.setStrategyUsed(strategyClass);
        performanceResult.setSchedulingState(state);
        performanceResult.setTotalNumNodesUsed(clusterResourceUtil.getNodesUsed());
        //get average time of run per topology
        long totalTimeRun = 0;
        //get average network closeness metric
        Double totalNetworkClosenessMetric = null;
        int numOfEntries = performanceResult.getPerTopologyPerformanceResultMap().values().size();
        assert (numOfEntries == this.topologies.getTopologies().size());
        for (PerTopologyPerformanceResult perTopologyPerformanceResult : performanceResult.getPerTopologyPerformanceResultMap().values()) {
            totalTimeRun += perTopologyPerformanceResult.getTimeToRun();
            if (perTopologyPerformanceResult.getNetworkClosenessMetric() != null) {
                if (totalNetworkClosenessMetric == null) {
                    totalNetworkClosenessMetric = perTopologyPerformanceResult.getNetworkClosenessMetric();
                } else {
                    totalNetworkClosenessMetric += perTopologyPerformanceResult.getNetworkClosenessMetric();
                }
            }
        }
        performanceResult.setAvgTimeToRun(totalTimeRun / numOfEntries);
        if (totalNetworkClosenessMetric != null) {
            performanceResult.setAvgNetworkClosenessMetric(totalNetworkClosenessMetric / (double) numOfEntries);
        }
        performanceResult.setStatsToPrint(clusterResourceUtil.toString());
        LOG.info(clusterResourceUtil.toString());
    }
}
