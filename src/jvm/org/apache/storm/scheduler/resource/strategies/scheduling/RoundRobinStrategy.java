package org.apache.storm.scheduler.resource.strategies.scheduling;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.RAS_Node;
import org.apache.storm.scheduler.resource.RAS_Nodes;
import org.apache.storm.scheduler.resource.SchedulingResult;
import org.apache.storm.scheduler.resource.SchedulingState;
import org.apache.storm.scheduler.resource.SchedulingStatus;
import org.apache.storm.scheduler.resource.strategies.scheduling.IStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jerrypeng on 8/16/16.
 */
public class RoundRobinStrategy implements IStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(RoundRobinStrategy.class);

    private Cluster _cluster;
    private Topologies _topologies;
    private RAS_Node refNode = null;
    private RAS_Nodes _nodes;
    private  List<WorkerSlot> _slots = new LinkedList<WorkerSlot>();

    @Override
    public void prepare(SchedulingState schedulingState) {
        _cluster = schedulingState.cluster;
        _topologies = schedulingState.topologies;
        _nodes = schedulingState.nodes;
        for (RAS_Node ras_node : _nodes.getNodes()) {
            _slots.addAll(ras_node.getFreeSlots());
        }
    }

    @Override
    public SchedulingResult schedule(TopologyDetails td) {
        if (_nodes.getNodes().size() <= 0) {
            LOG.warn("No available nodes to schedule tasks on!");
            return SchedulingResult.failure(SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES, "No available nodes to schedule tasks on!");
        }
        if (_slots.size() <= 0) {
            LOG.warn("No free slots to schedule tasks on!");
            return SchedulingResult.failure(SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES, "No free slots to schedule tasks on!");
        }
        Collection<ExecutorDetails> execs = td.getExecutors();
        Map<WorkerSlot, Collection<ExecutorDetails>> schedulerAssignmentMap = new HashMap<>();

        int i = 0;
        int scheduled = 0;
        for (ExecutorDetails exec : execs) {

            for (int j = 0 ; j < _slots.size(); j++) {
                int index = (i + j) % _slots.size();

                WorkerSlot slot = _slots.get(index);

                RAS_Node node = _nodes.getNodeById(slot.getNodeId());
                if (node.getAvailableCpuResources() >= td.getTotalCpuReqTask(exec) && node.getAvailableMemoryResources() >= td.getTotalMemReqTask(exec) && node.getFreeSlots().size() > 0 && checkWorkerConstraints(exec, slot, td, schedulerAssignmentMap)) {
                    node.consumeResourcesforTask(exec, td);
                    if (!schedulerAssignmentMap.containsKey(slot)) {
                        schedulerAssignmentMap.put(slot, new LinkedList<ExecutorDetails>());
                    }
                    schedulerAssignmentMap.get(slot).add(exec);
                    scheduled ++;
                    break;
                }
            }
            i++;
        }

        if (scheduled == td.getExecutors().size()) {
            return SchedulingResult.successWithMsg(schedulerAssignmentMap, "Scheduling Success by Resource Aware Round Robin Strategy");
        }
        return SchedulingResult.failure(SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES, "Not enough resources to schedule topology");
    }

    /**
     * Get the remaining amount memory that can be assigned to a worker given the set worker max heap size
     * @param ws
     * @param td
     * @param scheduleAssignmentMap
     * @return The remaining amount of memory
     */
    private Double getWorkerScheduledMemoryAvailable(WorkerSlot ws, TopologyDetails td, Map<WorkerSlot, Collection<ExecutorDetails>> scheduleAssignmentMap) {
        Double memScheduleUsed = this.getWorkerScheduledMemoryUse(ws, td, scheduleAssignmentMap);
        return td.getTopologyWorkerMaxHeapSize() - memScheduleUsed;
    }

    /**
     * Get the amount of memory already assigned to a worker
     * @param ws
     * @param td
     * @param scheduleAssignmentMap
     * @return the amount of memory
     */
    private Double getWorkerScheduledMemoryUse(WorkerSlot ws, TopologyDetails td, Map<WorkerSlot, Collection<ExecutorDetails>> scheduleAssignmentMap) {
        Double totalMem = 0.0;
        Collection<ExecutorDetails> execs = scheduleAssignmentMap.get(ws);
        if(execs != null) {
            for(ExecutorDetails exec : execs) {
                totalMem += td.getTotalMemReqTask(exec);
            }
        }
        return totalMem;
    }

    /**
     * Checks whether we can schedule an Executor exec on the worker slot ws
     * Only considers memory currently.  May include CPU in the future
     * @param exec
     * @param ws
     * @param td
     * @param scheduleAssignmentMap
     * @return a boolean: True denoting the exec can be scheduled on ws and false if it cannot
     */
    private boolean checkWorkerConstraints(ExecutorDetails exec, WorkerSlot ws, TopologyDetails td, Map<WorkerSlot, Collection<ExecutorDetails>> scheduleAssignmentMap) {
        boolean retVal = false;
        if(this.getWorkerScheduledMemoryAvailable(ws, td, scheduleAssignmentMap) >= td.getTotalMemReqTask(exec)) {
            retVal = true;
        }
        return retVal;
    }
}
