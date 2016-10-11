package org.apache.storm.scheduler.performance.metrics;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.Component;
import org.apache.storm.scheduler.resource.ResourceExtraUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by jerrypeng on 8/2/16.
 */
public class SchedulingNetworkMetric {
    public static final int INTRA_WORKER_DISTANCE = 0;
    public static final int INTER_WORKER_DISTANCE = 1;
    public static final int INTER_NODE_DISTANCE = 2;
    public static final int INTER_RACK_DISTANCE = 3;
    public static final String INTRA_WORKER_COMMUNICATION_KEY = "intra-worker";
    public static final String INTER_WORKER_COMMUNICATION_KEY = "inter-worker";
    public static final String INTER_NODE_COMMUNICATION_KEY = "inter-node";
    public static final String INTER_RACK_COMMUNICATION_KEY = "inter-rack";

    private final TopologyDetails topo;
    private final Map<ExecutorDetails, Map<String, Integer>> execNetworkDistanceMap = new HashMap<ExecutorDetails, Map<String, Integer>>();
    private final Map<String, Component> componentMap;

    //network closeness metric of a topology
    private double networkClosenessMetric;
    //number of nodes a topology has touched
    private int numNodes;
    //number of workers a topology has touched
    private int numWorkers;

    private static final Logger LOG = LoggerFactory.getLogger(SchedulingNetworkMetric.class);

    private int incrementCount = 0;


    public double getNetworkClosenessMetric() {
        return networkClosenessMetric;
    }

    public int getNumWorkers() {
        return numWorkers;
    }

    public int getNumNodes() {
        return numNodes;
    }

    public SchedulingNetworkMetric(Cluster cluster, TopologyDetails topo) {
        this.topo = topo;
        this.componentMap = this.topo.getComponents();
        initSchedulingNetworkMetrics(cluster);
    }

    public SchedulingNetworkMetric(Map<WorkerSlot,
            Set<ExecutorDetails>> workerToExecs, Map<ExecutorDetails, WorkerSlot> execToWorker,
                                   Map<String, Set<ExecutorDetails>> nodeToExecs, TopologyDetails topo, Map<String, String> supIdsToRack) {
        this.topo = topo;
        this.componentMap = this.topo.getComponents();
        initSchedulingNetworkMetrics(workerToExecs, execToWorker, nodeToExecs, supIdsToRack);
    }

    @Override
    public String toString() {
        StringBuilder output = new StringBuilder();
        output.append("\n/****** Statistics for scheduling of Topology " + topo.getName() + "******/\n");
        Integer avgScore = 0;
        for (Component componentObj : this.componentMap.values()) {
            output.append("Component: ").append(componentObj.id)
                    .append(" Parents: ").append(componentObj.parents)
                    .append(" Children: ").append(componentObj.children)
                    .append("\n");
            for (ExecutorDetails exec : componentObj.execs) {
                int score = getNetworkDistanceScore(this.execNetworkDistanceMap.get(exec));
                avgScore += score;
                output.append("->Executor: ").append(exec)
                        .append("--> Communication: ").append(this.execNetworkDistanceMap.get(exec).toString())
                        .append(" Score: ").append(score)
                        .append("\n");
            }
        }
        output.append("Overall avg network distance score: ").append(avgScore.doubleValue() / this.execNetworkDistanceMap.size());
        return output.toString();
    }

    private void initSchedulingNetworkMetrics(Map<WorkerSlot,
            Set<ExecutorDetails>> workerToExecs, Map<ExecutorDetails, WorkerSlot> execToWorker,
                                              Map<String, Set<ExecutorDetails>> nodeToExecs, Map<String, String> supIdsToRack) {
        calculateStats(workerToExecs, execToWorker, nodeToExecs, supIdsToRack);

    }

    private void initSchedulingNetworkMetrics(Cluster cluster) {
        SchedulerAssignment topoAssignments = cluster.getAssignments().get(this.topo.getId());
        Map<String, Set<ExecutorDetails>> nodeToExecs = new HashMap<String, Set<ExecutorDetails>>();
        Map<WorkerSlot, Set<ExecutorDetails>> workerToExecs = new HashMap<WorkerSlot, Set<ExecutorDetails>>();
        for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> entry : topoAssignments.getSlotToExecutors().entrySet()) {
            WorkerSlot ws = entry.getKey();
            workerToExecs.put(ws, new HashSet<ExecutorDetails>());
            workerToExecs.get(ws).addAll(entry.getValue());
        }

        Map<ExecutorDetails, WorkerSlot> execToWorker = topoAssignments.getExecutorToSlot();

        for (Map.Entry<WorkerSlot, Set<ExecutorDetails>> entry : workerToExecs.entrySet()) {
            WorkerSlot ws = entry.getKey();
            Set<ExecutorDetails> execs = entry.getValue();
            if (!nodeToExecs.containsKey(ws.getNodeId())) {
                nodeToExecs.put(ws.getNodeId(), new HashSet<ExecutorDetails>());
            }
            nodeToExecs.get(ws.getNodeId()).addAll(execs);
        }

        calculateStats(workerToExecs, execToWorker, nodeToExecs, ResourceExtraUtils.getSupIdToRack(cluster));
    }

    /**
     * Calculate Stats
     * @param workerToExecs
     * @param execToWorker
     * @param nodeToExecs
     * @param supIdsToRack ! Please note that its a map of Rack ids to Supervisor Ids NOT Hostnames !
     */
    private void calculateStats(Map<WorkerSlot,
            Set<ExecutorDetails>> workerToExecs, Map<ExecutorDetails, WorkerSlot> execToWorker,
                                Map<String, Set<ExecutorDetails>> nodeToExecs, Map<String, String> supIdsToRack) {
        Collection<ExecutorDetails> execs = execToWorker.keySet();
        for (ExecutorDetails exec : execs) {
            //initialize
            this.execNetworkDistanceMap.put(exec, new HashMap<String, Integer>());
            this.execNetworkDistanceMap.get(exec).put(INTRA_WORKER_COMMUNICATION_KEY, 0);
            this.execNetworkDistanceMap.get(exec).put(INTER_WORKER_COMMUNICATION_KEY, 0);
            this.execNetworkDistanceMap.get(exec).put(INTER_NODE_COMMUNICATION_KEY, 0);
            this.execNetworkDistanceMap.get(exec).put(INTER_RACK_COMMUNICATION_KEY, 0);

            String component = topo.getExecutorToComponent().get(exec);
            //get worker executor scheduled on
            WorkerSlot ws = execToWorker.get(exec);
            //get node executor scheduled on
            String nodeId = ws.getNodeId();

            Component componentObj = this.componentMap.get(component);
            //LOG.info("component: {} componentMap: {}", component, componentMap);
            if (componentObj != null) {
                for (String childrenComp : componentObj.children) {
                    for (ExecutorDetails childrenCompExec : this.componentMap.get(childrenComp).execs) {

                        //check if childrenCompExec, a executor that is directly downstream from exec, is scheduled on the same worker as exec
                        if (workerToExecs.get(ws).contains(childrenCompExec)) {
                            incrementExecNetworkDistanceMapEntry(this.execNetworkDistanceMap, exec, INTRA_WORKER_COMMUNICATION_KEY);
                        }
                        //check if childrenCompExec, a executor that is directly downstream from exec, is scheduled on the same node as exec
                        else if (nodeToExecs.get(nodeId).contains(childrenCompExec)) {
                            incrementExecNetworkDistanceMapEntry(this.execNetworkDistanceMap, exec, INTER_WORKER_COMMUNICATION_KEY);
                        }
                        // same rack inter-node communication
                        else if (supIdsToRack.get(nodeId).equals(supIdsToRack.get(execToWorker.get(childrenCompExec).getNodeId()))) {
                            incrementExecNetworkDistanceMapEntry(this.execNetworkDistanceMap, exec, INTER_NODE_COMMUNICATION_KEY);
                        }
                        // inter-rack communication
                        else {
                            incrementExecNetworkDistanceMapEntry(this.execNetworkDistanceMap, exec, INTER_RACK_COMMUNICATION_KEY);
                        }
                    }
                }
            } else {
                //LOG.debug("Cannot find component {} to calculate stats", component);
            }
        }

        // valid anymore
//        if (this.topo.getComponents().size() > 1) {
//            assert (this.incrementCount != 0);
//        }

        int sumScore = 0;
        for (ExecutorDetails exec : execToWorker.keySet()) {
            sumScore += getNetworkDistanceScore(this.execNetworkDistanceMap.get(exec));
        }

        this.networkClosenessMetric = (double) sumScore / (double) this.execNetworkDistanceMap.size();
        this.numNodes = nodeToExecs.keySet().size();
        this.numWorkers = workerToExecs.keySet().size();
    }


    private static int getNetworkDistanceScore(Map<String, Integer> scores) {
        return (scores.get(INTRA_WORKER_COMMUNICATION_KEY) * INTRA_WORKER_DISTANCE)
                + (scores.get(INTER_WORKER_COMMUNICATION_KEY) * INTER_WORKER_DISTANCE)
                + (scores.get(INTER_NODE_COMMUNICATION_KEY) * INTER_NODE_DISTANCE)
                + (scores.get(INTER_RACK_COMMUNICATION_KEY) * INTER_RACK_DISTANCE);
    }

    private void incrementExecNetworkDistanceMapEntry(Map<ExecutorDetails, Map<String, Integer>> execNetworkDistanceMap, ExecutorDetails exec, String key) {
        this.incrementCount ++;

        if (!execNetworkDistanceMap.containsKey(exec)) {
            execNetworkDistanceMap.put(exec, new HashMap<String, Integer>());
        }
        Map<String, Integer> distanceMap = execNetworkDistanceMap.get(exec);
        if (!distanceMap.containsKey(key)) {
            distanceMap.put(key, 0);
        }
        Integer existingValue = distanceMap.get(key);
        distanceMap.put(key, existingValue + 1);
    }
}
