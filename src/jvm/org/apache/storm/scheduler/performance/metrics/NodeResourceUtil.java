package org.apache.storm.scheduler.performance.metrics;

import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.RAS_Node;

import java.util.Collection;
import java.util.Map;

/**
 * Created by jerrypeng on 8/1/16.
 */
public class NodeResourceUtil {

    private RAS_Node node;
    private Topologies topologies;
    private Map<String, Map<WorkerSlot, Collection<ExecutorDetails>>> scheduling;

    private double avgCpuUtil;
    private double avgMemUtil;
    private double avgResourceUtil;
    private String rackId;

    public String getRackId() {
        return rackId;
    }

    public void setRackId(String rackId) {
        this.rackId = rackId;
    }

    public double getAvgCpuUtil() {
        return avgCpuUtil;
    }

    public double getAvgMemUtil() {
        return avgMemUtil;
    }

    public double getAvgResourceUtil() {
        return avgResourceUtil;
    }

    public NodeResourceUtil(RAS_Node node, Topologies topologies, Map<String, Map<WorkerSlot, Collection<ExecutorDetails>>> scheduling, String rackId) {
        this.node = node;
        double totalMem = this.node.getTotalMemoryResources();
        double availMem = this.node.getAvailableMemoryResources();
        double totalCpu = this.node.getTotalCpuResources();
        double availCpu = this.node.getAvailableCpuResources();

        this.avgCpuUtil = (1.0 - availMem / totalMem) * 100.0;
        this.avgMemUtil = (1.0 - availCpu / totalCpu) * 100.0;
        this.avgResourceUtil = (this.avgCpuUtil + this.avgMemUtil) / 2.0;
        this.scheduling = scheduling;
        this.topologies = topologies;
        this.rackId = rackId;
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();
        sb.append("/** Node: " + this.node.getHostname() + "-" + this.node.getId()
                + " [Mem Total: " + this.node.getTotalMemoryResources()
                + " Avail: " + this.node.getAvailableMemoryResources()
                + " Util: " + this.avgMemUtil + "%"
                + "] [CPU Total: " + this.node.getTotalCpuResources()
                + " Avail: " + this.node.getAvailableCpuResources()
                + " Util: " + this.avgCpuUtil + "%" + "] on Rack " + this.rackId + " **/\n");
        for (Map.Entry<String, Map<WorkerSlot, Collection<ExecutorDetails>>> entry : this.scheduling.entrySet()) {
            String topoId = entry.getKey();
            Map<WorkerSlot, Collection<ExecutorDetails>> assignment = entry.getValue();
            sb.append("\t-->Topology: " + topoId + "\n");
            for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> ws : assignment.entrySet()) {
                sb.append("\t\t->Slot [" + ws.getKey().getPort() + "] -> " + ws.getValue() + " : ");
                StringBuilder comps = new StringBuilder();
                comps.append("[ ");
                for (ExecutorDetails exec : ws.getValue()) {
                    comps.append(this.topologies.getById(topoId).getExecutorToComponent().get(exec)).append(", ");
                }
                comps.append("]");
                sb.append(comps.toString());
            }
        }
        return sb.toString();
    }
}
