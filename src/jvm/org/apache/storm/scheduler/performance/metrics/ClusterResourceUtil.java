package org.apache.storm.scheduler.performance.metrics;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.RAS_Node;
import org.apache.storm.scheduler.resource.RAS_Nodes;
import org.apache.storm.scheduler.performance.PerformanceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Created by jerrypeng on 8/1/16.
 */
public class ClusterResourceUtil {
    private int nodesUsed;
    //set to null when no valid value AKA nodesUsed == 0
    private Double avgCpuUtil = null;
    private Double avgMemUtil = null;
    private Double avgUtil = null;

    public int getNodesUsed() {
        return nodesUsed;
    }

    public Double getAvgCpuUtil() {
        return avgCpuUtil;
    }

    public Double getAvgMemUtil() {
        return avgMemUtil;
    }

    public Double getAvgUtil() {
        return avgUtil;
    }


    public Map<String, NodeResourceUtil> nodeResourceUtils = new HashMap<String, NodeResourceUtil>();

    private static final Logger LOG = LoggerFactory.getLogger(ClusterResourceUtil.class);


    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("\n/****** Statistics for Cluster ******/\n");
        for (NodeResourceUtil entry : this.nodeResourceUtils.values()) {
            str.append(entry.toString()).append("\n");
        }
        str.append("# of nodes used: " + this.nodesUsed + " Avg Utilization: " + this.avgUtil + "%");
        return str.toString();
    }

    public ClusterResourceUtil(Cluster cluster, Topologies topologies) {

        RAS_Nodes nodes = new RAS_Nodes(cluster, topologies);
        //nodeId->topologyId->WorkerSlot->Execs
        Map<String, Map<String, Map<WorkerSlot, Collection<ExecutorDetails>>>> schedulingMap = new HashMap<String, Map<String, Map<WorkerSlot, Collection<ExecutorDetails>>>>();
        for (TopologyDetails topo : topologies.getTopologies()) {
            if (cluster.getAssignmentById(topo.getId()) != null) {
                for (Map.Entry<ExecutorDetails, WorkerSlot> entry : cluster.getAssignmentById(topo.getId()).getExecutorToSlot().entrySet()) {
                    WorkerSlot slot = entry.getValue();
                    String nodeId = slot.getNodeId();
                    ExecutorDetails exec = entry.getKey();
                    if (!schedulingMap.containsKey(nodeId)) {
                        schedulingMap.put(nodeId, new HashMap<String, Map<WorkerSlot, Collection<ExecutorDetails>>>());
                    }
                    if (schedulingMap.get(nodeId).containsKey(topo.getId()) == false) {
                        schedulingMap.get(nodeId).put(topo.getId(), new HashMap<WorkerSlot, Collection<ExecutorDetails>>());
                    }
                    if (schedulingMap.get(nodeId).get(topo.getId()).containsKey(slot) == false) {
                        schedulingMap.get(nodeId).get(topo.getId()).put(slot, new LinkedList<ExecutorDetails>());
                    }
                    schedulingMap.get(nodeId).get(topo.getId()).get(slot).add(exec);
                }
            }
        }
        this.nodesUsed = schedulingMap.size();
        initNodeResouceUtils(cluster, topologies, schedulingMap);
        double sumCpuUtil = 0.0;
        double sumMemUtil = 0.0;

        for (NodeResourceUtil nodeResourceUtil : this.nodeResourceUtils.values()) {
            sumCpuUtil += nodeResourceUtil.getAvgCpuUtil();
            sumMemUtil += nodeResourceUtil.getAvgMemUtil();
        }

        if (this.nodesUsed > 0.0) {
            this.avgCpuUtil = sumCpuUtil / this.nodesUsed;
            this.avgMemUtil = sumMemUtil / this.nodesUsed;
            this.avgUtil = (this.avgCpuUtil + this.avgMemUtil) / 2.0;
        }
    }

    private void initNodeResouceUtils(Cluster cluster, Topologies topologies, Map<String, Map<String, Map<WorkerSlot, Collection<ExecutorDetails>>>> schedulingMap) {
        RAS_Nodes nodes = new RAS_Nodes(cluster, topologies);
        Map<String, String> supIdsToRack = PerformanceUtils.getSupIdToRack(cluster);

        for (Map.Entry<String, Map<String, Map<WorkerSlot, Collection<ExecutorDetails>>>> entry : schedulingMap.entrySet()) {
            String nodeId = entry.getKey();
            SupervisorDetails sup = cluster.getSupervisorById(entry.getKey());
            RAS_Node node = nodes.getNodeById(sup.getId());
            this.nodeResourceUtils.put(nodeId, new NodeResourceUtil(node, topologies, schedulingMap.get(nodeId), supIdsToRack.get(nodeId)));
        }
    }
}
