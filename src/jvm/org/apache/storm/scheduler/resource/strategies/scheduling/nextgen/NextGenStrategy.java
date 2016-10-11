package org.apache.storm.scheduler.resource.strategies.scheduling.nextgen;

import org.apache.storm.Config;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.Component;
import org.apache.storm.scheduler.resource.RAS_Node;
import org.apache.storm.scheduler.resource.RAS_Nodes;
import org.apache.storm.scheduler.resource.SchedulingResult;
import org.apache.storm.scheduler.resource.SchedulingState;
import org.apache.storm.scheduler.resource.SchedulingStatus;
import org.apache.storm.scheduler.resource.strategies.scheduling.IStrategy;

import org.apache.storm.scheduler.resource.ResourceExtraUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Created by jerrypeng on 7/27/16.
 */
public class NextGenStrategy implements IStrategy{
    private static final Logger LOG = LoggerFactory.getLogger(NextGenStrategy.class);
    private Cluster _cluster;
    private Topologies _topologies;
    private Map<String, List<String>> _clusterInfo;
    private RAS_Nodes _nodes;

    private final double CPU_WEIGHT = 1.0;
    private final double MEM_WEIGHT = 1.0;
    private final double NETWORK_WEIGHT = 1.0;

    public void prepare (SchedulingState schedulingState) {
        _cluster = schedulingState.cluster;
        _topologies = schedulingState.topologies;
        _nodes = schedulingState.nodes;
        _clusterInfo = schedulingState.cluster.getNetworkTopography();
        LOG.debug(this.getClusterInfo());
    }

    //the returned TreeMap keeps the Components sorted
    private TreeMap<Integer, List<ExecutorDetails>> getPriorityToExecutorDetailsListMap(
            Queue<Component> ordered__Component_list, Collection<ExecutorDetails> unassignedExecutors) {
        TreeMap<Integer, List<ExecutorDetails>> retMap = new TreeMap<>();
        Integer rank = 0;
        for (Component ras_comp : ordered__Component_list) {
            retMap.put(rank, new ArrayList<ExecutorDetails>());
            for(ExecutorDetails exec : ras_comp.execs) {
                if(unassignedExecutors.contains(exec)) {
                    retMap.get(rank).add(exec);
                }
            }
            rank++;
        }
        return retMap;
    }

    public SchedulingResult schedule(TopologyDetails td) {
        LOG.debug("max heap: {}", td.getConf().get(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB));
        if (_nodes.getNodes().size() <= 0) {
            LOG.warn("No available nodes to schedule tasks on!");
            return SchedulingResult.failure(SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES, "No available nodes to schedule tasks on!");
        }
        Collection<ExecutorDetails> unassignedExecutors = _cluster.getUnassignedExecutors(td);
        Map<WorkerSlot, Collection<ExecutorDetails>> schedulerAssignmentMap = new HashMap<>();
        LOG.debug("ExecutorsNeedScheduling: {}", unassignedExecutors);
        Collection<ExecutorDetails> scheduledTasks = new ArrayList<>();
        List<Component> spouts = this.getSpouts(td);

        if (spouts.size() == 0) {
            LOG.error("Cannot find a Spout!");
            return SchedulingResult.failure(SchedulingStatus.FAIL_INVALID_TOPOLOGY, "Cannot find a Spout!");
        }

        Queue<Component> ordered__Component_list = bfs(td, spouts);

        Map<Integer, List<ExecutorDetails>> priorityToExecutorMap = getPriorityToExecutorDetailsListMap(ordered__Component_list, unassignedExecutors);
        Collection<ExecutorDetails> executorsNotScheduled = new HashSet<>(unassignedExecutors);
        Integer longestPriorityListSize = this.getLongestPriorityListSize(priorityToExecutorMap);
        //Pick the first executor with priority one, then the 1st exec with priority 2, so on an so forth.
        //Once we reach the last priority, we go back to priority 1 and schedule the second task with priority 1.
        for (int i = 0; i < longestPriorityListSize; i++) {
            for (Map.Entry<Integer, List<ExecutorDetails>> entry : priorityToExecutorMap.entrySet()) {
                Iterator<ExecutorDetails> it = entry.getValue().iterator();
                if (it.hasNext()) {
                    ExecutorDetails exec = it.next();
                    LOG.debug("\n\nAttempting to schedule: {} of component {}[ REQ {} ] with rank {}",
                            new Object[] { exec, td.getExecutorToComponent().get(exec),
                                    td.getTaskResourceReqList(exec), entry.getKey() });
                    scheduleExecutor(exec, td, schedulerAssignmentMap, scheduledTasks);
                    it.remove();
                }
            }
        }

        executorsNotScheduled.removeAll(scheduledTasks);
        LOG.debug("/* Scheduling left over task (most likely sys tasks) */");
        // schedule left over system tasks
        for (ExecutorDetails exec : executorsNotScheduled) {
            scheduleExecutor(exec, td, schedulerAssignmentMap, scheduledTasks);
        }

        SchedulingResult result;
        executorsNotScheduled.removeAll(scheduledTasks);
        if (executorsNotScheduled.size() > 0) {
            LOG.error("Not all executors successfully scheduled: {}",
                    executorsNotScheduled);
            schedulerAssignmentMap = null;
            result = SchedulingResult.failure(SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES,
                    (td.getExecutors().size() - unassignedExecutors.size()) + "/" + td.getExecutors().size() + " executors scheduled");
        } else {
            LOG.debug("All resources successfully scheduled!");
            result = SchedulingResult.successWithMsg(schedulerAssignmentMap, "Fully Scheduled by NextGenStrategy");
        }
        if (schedulerAssignmentMap == null) {
            LOG.error("Topology {} not successfully scheduled!", td.getId());
        }
        return result;
    }

    private void scheduleExecutor(ExecutorDetails exec, TopologyDetails td, Map<WorkerSlot,
            Collection<ExecutorDetails>> schedulerAssignmentMap, Collection<ExecutorDetails> scheduledTasks) {
        WorkerSlot targetSlot = this.findWorkerForExec(exec, td, schedulerAssignmentMap);
        if (targetSlot != null) {
            RAS_Node targetNode = this.idToNode(targetSlot.getNodeId());
            if (!schedulerAssignmentMap.containsKey(targetSlot)) {
                schedulerAssignmentMap.put(targetSlot, new LinkedList<ExecutorDetails>());
            }

            schedulerAssignmentMap.get(targetSlot).add(exec);
            targetNode.consumeResourcesforTask(exec, td);

            scheduledTasks.add(exec);
            LOG.debug("TASK {} assigned to Node: {} avail [ mem: {} cpu: {} ] total [ mem: {} cpu: {} ] on slot: {} on Rack: {}", exec,
                    targetNode.getHostname(), targetNode.getAvailableMemoryResources(),
                    targetNode.getAvailableCpuResources(), targetNode.getTotalMemoryResources(),
                    targetNode.getTotalCpuResources(), targetSlot, nodeToRack(targetNode));
        } else {
            LOG.error("Not Enough Resources to schedule Task {}", exec);
        }
    }

    private WorkerSlot findWorkerForExec(ExecutorDetails exec, TopologyDetails td, Map<WorkerSlot, Collection<ExecutorDetails>> scheduleAssignmentMap) {
        WorkerSlot ws = null;

        // iterate through an ordered list of all racks available to make sure we cannot schedule the first executor in any rack before we "give up"
        // the list is ordered in decreasing order of effective resources. With the rack in the front of the list having the most effective resources.
        for (ObjectResources rack : sortRacks(td.getId(), scheduleAssignmentMap)) {
            ws = this.getBestWorker(exec, td, rack.id, scheduleAssignmentMap);
            if (ws != null) {
                LOG.debug("best rack: {}", rack.id);
                break;
            }
        }
        return ws;
    }

    private WorkerSlot getBestWorker(ExecutorDetails exec, TopologyDetails td, String rackId, Map<WorkerSlot, Collection<ExecutorDetails>> scheduleAssignmentMap) {
        List<RAS_Node> nodes;
        if(rackId != null) {
            nodes = this.getAvailableNodesFromRack(rackId);

        } else {
            nodes = this.getAvailableNodes();
        }

        double taskMem = td.getTotalMemReqTask(exec);
        double taskCPU = td.getTotalCpuReqTask(exec);

        TreeSet<ObjectResources> sortedNodeResources = sortNodes(nodes, rackId, td.getId(), scheduleAssignmentMap);
        for(ObjectResources nodeResources : sortedNodeResources) {
            RAS_Node n = this._nodes.getNodeById(nodeResources.id);
            if (n.getAvailableCpuResources() >= taskCPU && n.getAvailableMemoryResources() >= taskMem && n.getFreeSlots().size() > 0) {
                for (WorkerSlot ws : n.getFreeSlots()) {
                    if (checkWorkerConstraints(exec, ws, td, scheduleAssignmentMap)) {
                        return ws;
                    }
                }
            }
        }
        return null;
    }

    interface ExistingScheduleFunc {
        public int getNumExistingSchedule(String objectId);
    }

    class AllResources {
        List<ObjectResources> objectResources = new LinkedList<ObjectResources>();
        Double availMemResourcesOverall = 0.0;
        Double totalMemResourcesOverall = 0.0;
        Double availCpuResourcesOverall = 0.0;
        Double totalCpuResourcesOverall = 0.0;
        Integer freeSlotsOverall = 0;
        Integer totalSlotsOverall = 0;
        String identifier;
        public AllResources(String identifier) {
            this.identifier = identifier;
        }
    }
    /**
     * class to keep track of resources on a rack
     */
    class ObjectResources {
        String id;
        Double availMem = 0.0;
        Double totalMem = 0.0;
        Double availCpu = 0.0;
        Double totalCpu = 0.0;
        Integer freeSlots = 0;
        Integer totalSlots = 0;
        double effectiveResources = 0.0;
        public ObjectResources(String id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return this.id;
        }
    }

    public TreeSet<ObjectResources> sortNodes(List<RAS_Node> availNodes, String rackId, final String topoId, final Map<WorkerSlot, Collection<ExecutorDetails>> scheduleAssignmentMap) {
        AllResources allResources = new AllResources("RACK");
        List<ObjectResources> nodes = allResources.objectResources;
        final Map<String, String> nodeIdToRackId = new HashMap<String, String>();

        for (RAS_Node ras_node : availNodes) {
            String nodeId = ras_node.getId();
            ObjectResources node = new ObjectResources(nodeId);

            double availMem = ras_node.getAvailableMemoryResources();
            double availCpu = ras_node.getAvailableCpuResources();
            int freeSlots = ras_node.totalSlotsFree();
            double totalMem = ras_node.getTotalMemoryResources();
            double totalCpu = ras_node.getTotalCpuResources();
            int totalSlots = ras_node.totalSlots();

            node.availMem = availMem;
            node.totalMem = totalMem;
            node.availCpu = availCpu;
            node.totalCpu = totalCpu;
            node.freeSlots = freeSlots;
            node.totalSlots = totalSlots;
            nodes.add(node);

            allResources.availMemResourcesOverall += availMem;
            allResources.availCpuResourcesOverall += availCpu;

            allResources.totalMemResourcesOverall += totalMem;
            allResources.totalCpuResourcesOverall += totalCpu;
        }
        //don't care about slots because strategy is not slot constrained.  Can put any number of executors in workers up to TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB.
        allResources.totalSlotsOverall = null;
        allResources.freeSlotsOverall = null;

        LOG.debug("Rack {}: Overall Avail [ CPU {} MEM {} Slots {} ] Total [ CPU {} MEM {} Slots {} ]",
                rackId, allResources.availCpuResourcesOverall, allResources.availMemResourcesOverall, allResources.freeSlotsOverall, allResources.totalCpuResourcesOverall, allResources.totalMemResourcesOverall, allResources.totalSlotsOverall);

        return sortObjectResources(allResources, new ExistingScheduleFunc() {
            @Override
            public int getNumExistingSchedule(String objectId) {

                //Get execs already assigned in rack
                Collection<ExecutorDetails> execs = new LinkedList<ExecutorDetails>();
                if (_cluster.getAssignmentById(topoId) != null) {
                    for (Map.Entry<ExecutorDetails, WorkerSlot> entry : _cluster.getAssignmentById(topoId).getExecutorToSlot().entrySet()) {
                        WorkerSlot workerSlot = entry.getValue();
                        ExecutorDetails exec = entry.getKey();
                        if (workerSlot.getNodeId().equals(objectId)) {
                            execs.add(exec);
                        }
                    }
                }
                // get execs already scheduled in the current scheduling
                for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> entry : scheduleAssignmentMap.entrySet()) {

                    WorkerSlot workerSlot = entry.getKey();
                    if (workerSlot.getNodeId().equals(objectId)) {
                        execs.addAll(entry.getValue());
                    }
                }
                return execs.size();
            }
        });
    }

    /**
     * Sort racks
     * @param topoId topology id
     * @param scheduleAssignmentMap calculated assignments so far
     * @return a sorted list of racks
     * Racks are sorted by two criteria. 1) the number executors of the topology that needs to be scheduled is already on the rack in descending order.
     * The reasoning to sort based on  criterion 1 is so we schedule the rest of a topology on the same rack as the existing executors of the topology.
     * 2) the subordinate/subservient resource availability percentage of a rack in descending order
     * We calculate the resource availability percentage by dividing the resource availability on the rack by the resource availability of the entire cluster
     * By doing this calculation, racks that have exhausted or little of one of the resources mentioned above will be ranked after racks that have more balanced resource availability.
     * So we will be less likely to pick a rack that have a lot of one resource but a low amount of another.
     */
    TreeSet<ObjectResources> sortRacks(final String topoId, final Map<WorkerSlot, Collection<ExecutorDetails>> scheduleAssignmentMap) {
        AllResources allResources = new AllResources("Cluster");
        List<ObjectResources> racks = allResources.objectResources;

        final Map<String, String> nodeIdToRackId = new HashMap<String, String>();

        for (Map.Entry<String, List<String>> entry : _clusterInfo.entrySet()) {
            String rackId = entry.getKey();
            List<String> nodeIds = entry.getValue();
            ObjectResources rack = new ObjectResources(rackId);
            racks.add(rack);
            for (String nodeId : nodeIds) {
                RAS_Node node = _nodes.getNodeById(this.NodeHostnameToId(nodeId));
                double availMem = node.getAvailableMemoryResources();
                double availCpu = node.getAvailableCpuResources();
//                int freeSlots = node.totalSlotsFree();
                double totalMem = node.getTotalMemoryResources();
                double totalCpu = node.getTotalCpuResources();
//                int totalSlots = node.totalSlots();
//
                rack.availMem += availMem;
                rack.totalMem += totalMem;
                rack.availCpu += availCpu;
                rack.totalCpu += totalCpu;
//                rack.freeSlots += freeSlots;
//                rack.totalSlots += totalSlots;
                nodeIdToRackId.put(nodeId, rack.id);

                allResources.availMemResourcesOverall += availMem;
                allResources.availCpuResourcesOverall += availCpu;
//                allResources.freeSlotsOverall += freeSlots;

                allResources.totalMemResourcesOverall += totalMem;
                allResources.totalCpuResourcesOverall += totalCpu;
//                allResources.totalSlotsOverall += totalSlots;
            }
            allResources.totalSlotsOverall = null;
            allResources.freeSlotsOverall = null;
        }
        LOG.debug("Cluster Overall Avail [ CPU {} MEM {} Slots {} ] Total [ CPU {} MEM {} Slots {} ]",
                allResources.availCpuResourcesOverall, allResources.availMemResourcesOverall, allResources.freeSlotsOverall, allResources.totalCpuResourcesOverall, allResources.totalMemResourcesOverall, allResources.totalSlotsOverall);

        return sortObjectResources(allResources, new ExistingScheduleFunc() {
            @Override
            public int getNumExistingSchedule(String objectId) {

                String rackId = objectId;
                //Get execs already assigned in rack
                Collection<ExecutorDetails> execs = new LinkedList<ExecutorDetails>();
                if (_cluster.getAssignmentById(topoId) != null) {
                    for (Map.Entry<ExecutorDetails, WorkerSlot> entry : _cluster.getAssignmentById(topoId).getExecutorToSlot().entrySet()) {
                        String nodeId = entry.getValue().getNodeId();
                        String hostname = idToNode(nodeId).getHostname();
                        ExecutorDetails exec = entry.getKey();
                        if (nodeIdToRackId.get(hostname) != null && nodeIdToRackId.get(hostname).equals(rackId)) {
                            execs.add(exec);
                        }
                    }
                }
                // get execs already scheduled in the current scheduling
                for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> entry : scheduleAssignmentMap.entrySet()) {
                    WorkerSlot workerSlot = entry.getKey();
                    String nodeId = workerSlot.getNodeId();
                    String hostname = idToNode(nodeId).getHostname();
                    if (nodeIdToRackId.get(hostname).equals(rackId)) {
                        execs.addAll(entry.getValue());
                    }
                }
                return execs.size();
            }
        });
    }

    public TreeSet<ObjectResources> sortObjectResources(final AllResources allResources, final ExistingScheduleFunc existingScheduleFunc) {

        for (ObjectResources objectResources : allResources.objectResources) {
            StringBuilder sb = new StringBuilder();
            if ((allResources.availCpuResourcesOverall != null && allResources.availCpuResourcesOverall <= 0.0)
                    || (allResources.availMemResourcesOverall != null && allResources.availMemResourcesOverall <= 0.0)
                    || (allResources.freeSlotsOverall != null && allResources.freeSlotsOverall <= 0.0)) {
                objectResources.effectiveResources = 0.0;
            } else {
                List<Double> values = new LinkedList<Double>();
                if (allResources.availCpuResourcesOverall != null) {
                    double value = (objectResources.availCpu / allResources.availCpuResourcesOverall) * 100.0;
                    values.add(value);

                    sb.append(String.format("CPU %f(%f%%) ", objectResources.availCpu, value));
                }
                if (allResources.availMemResourcesOverall != null) {
                    double value = (objectResources.availMem/allResources.availMemResourcesOverall) * 100.0;
                    values.add(value);
                    sb.append(String.format("MEM %f(%f%%) ", objectResources.availMem, value));

                }
                if (allResources.freeSlotsOverall != null) {
                    double value = ((double) objectResources.freeSlots / (double) allResources.freeSlotsOverall) * 100.0;
                    values.add(value);
                    sb.append(String.format("Slots %d(%f%%) ", objectResources.freeSlots, value));
                }

                objectResources.effectiveResources =  Collections.min(values);
            }
            LOG.debug("{}: Avail [ {} ] Total [ CPU {} MEM {} Slots {} ] effective resources: {}",
                    objectResources.id, sb.toString(),
                    objectResources.totalCpu, objectResources.totalMem,
                    objectResources.totalSlots, objectResources.effectiveResources);
        }

        TreeSet<ObjectResources> sortedObjectResources = new TreeSet<ObjectResources>(new Comparator<ObjectResources>() {
            @Override
            public int compare(ObjectResources o1, ObjectResources o2) {

                int execsScheduled1 = existingScheduleFunc.getNumExistingSchedule(o1.id);
                int execsScheduled2 = existingScheduleFunc.getNumExistingSchedule(o2.id);
                //LOG.info("{} - {} vs {} - {}", o1.id, execsScheduled1, o2.id, execsScheduled2);
                if (execsScheduled1 > execsScheduled2) {
                    return -1;
                } else if (execsScheduled1 < execsScheduled2) {
                    return 1;
                } else {
                    if (o1.effectiveResources > o2.effectiveResources) {
                        return -1;
                    } else if (o1.effectiveResources < o2.effectiveResources) {
                        return 1;
                    }
                    else {
                        List<Double> o1_values = new LinkedList<Double>();
                        List<Double> o2_values = new LinkedList<Double>();
                        if (allResources.availCpuResourcesOverall != null) {
                            o1_values.add((o1.availCpu / allResources.availCpuResourcesOverall) * 100.0);
                            o2_values.add((o2.availCpu / allResources.availCpuResourcesOverall) * 100.0);
                        }

                        if (allResources.availMemResourcesOverall != null) {
                            o1_values.add((o1.availMem/allResources.availMemResourcesOverall) * 100.0);
                            o2_values.add((o2.availMem/allResources.availMemResourcesOverall) * 100.0);
                        }

                        if (allResources.freeSlotsOverall != null) {
                            o1_values.add(((double) o1.freeSlots / (double) allResources.freeSlotsOverall) * 100.0);
                            o2_values.add(((double) o2.freeSlots / (double) allResources.freeSlotsOverall) * 100.0);
                        }

                        double o1_avg = ResourceExtraUtils.avg(o1_values);
                        double o2_avg = ResourceExtraUtils.avg(o2_values);

                        if (o1_avg > o2_avg) {
                            return -1;
                        } else if (o1_avg < o2_avg) {
                            return 1;
                        } else {
                            return o1.id.compareTo(o2.id);
                        }
                    }
                }
            }
        });
        sortedObjectResources.addAll(allResources.objectResources);
        LOG.debug("Sorted Object Resources: {}", sortedObjectResources);
        return sortedObjectResources;
    }

    private Double distToNode(RAS_Node src, RAS_Node dest) {
        if (src.getId().equals(dest.getId())) {
            return 0.0;
        } else if (this.nodeToRack(src).equals(this.nodeToRack(dest))) {
            return 0.5;
        } else {
            return 1.0;
        }
    }

    private String nodeToRack(RAS_Node node) {
        for (Map.Entry<String, List<String>> entry : _clusterInfo
                .entrySet()) {
            if (entry.getValue().contains(node.getHostname())) {
                return entry.getKey();
            }
        }
        LOG.error("Node: {} not found in any racks", node.getHostname());
        return null;
    }

    private List<RAS_Node> getAvailableNodes() {
        LinkedList<RAS_Node> nodes = new LinkedList<>();
        for (String rackId : _clusterInfo.keySet()) {
            nodes.addAll(this.getAvailableNodesFromRack(rackId));
        }
        return nodes;
    }

    private List<RAS_Node> getAvailableNodesFromRack(String rackId) {
        List<RAS_Node> retList = new ArrayList<>();
        for (String node_id : _clusterInfo.get(rackId)) {
            retList.add(_nodes.getNodeById(this
                    .NodeHostnameToId(node_id)));
        }
        return retList;
    }

    private List<WorkerSlot> getAvailableWorkersFromRack(String rackId) {
        List<RAS_Node> nodes = this.getAvailableNodesFromRack(rackId);
        List<WorkerSlot> workers = new LinkedList<>();
        for(RAS_Node node : nodes) {
            workers.addAll(node.getFreeSlots());
        }
        return workers;
    }

    private List<WorkerSlot> getAvailableWorker() {
        List<WorkerSlot> workers = new LinkedList<>();
        for (String rackId : _clusterInfo.keySet()) {
            workers.addAll(this.getAvailableWorkersFromRack(rackId));
        }
        return workers;
    }

    /**
     * Breadth first traversal of the topology DAG
     * @param td
     * @param spouts
     * @return A partial ordering of components
     */
    private Queue<Component> bfs(TopologyDetails td, List<Component> spouts) {
        // Since queue is a interface
        Queue<Component> ordered__Component_list = new LinkedList<Component>();
        HashSet<String> visited = new HashSet<>();

        /* start from each spout that is not visited, each does a breadth-first traverse */
        for (Component spout : spouts) {
            if (!visited.contains(spout.id)) {
                Queue<Component> queue = new LinkedList<>();
                visited.add(spout.id);
                queue.offer(spout);
                while (!queue.isEmpty()) {
                    Component comp = queue.poll();
                    ordered__Component_list.add(comp);
                    List<String> neighbors = new ArrayList<>();
                    neighbors.addAll(comp.children);
                    neighbors.addAll(comp.parents);
                    for (String nbID : neighbors) {
                        if (!visited.contains(nbID)) {
                            Component child = td.getComponents().get(nbID);
                            visited.add(nbID);
                            queue.offer(child);
                        }
                    }
                }
            }
        }
        return ordered__Component_list;
    }

    private List<Component> getSpouts(TopologyDetails td) {
        List<Component> spouts = new ArrayList<>();

        for (Component c : td.getComponents().values()) {
            if (c.type == Component.ComponentType.SPOUT) {
                spouts.add(c);
            }
        }
        return spouts;
    }

    private Integer getLongestPriorityListSize(Map<Integer, List<ExecutorDetails>> priorityToExecutorMap) {
        Integer mostNum = 0;
        for (List<ExecutorDetails> execs : priorityToExecutorMap.values()) {
            Integer numExecs = execs.size();
            if (mostNum < numExecs) {
                mostNum = numExecs;
            }
        }
        return mostNum;
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

    /**
     * Get the amount of resources available and total for each node
     * @return a String with cluster resource info for debug
     */
    private String getClusterInfo() {
        String retVal = "Cluster info:\n";
        for(Map.Entry<String, List<String>> clusterEntry : _clusterInfo.entrySet()) {
            String clusterId = clusterEntry.getKey();
            retVal += "Rack: " + clusterId + "\n";
            for(String nodeHostname : clusterEntry.getValue()) {
                RAS_Node node = this.idToNode(this.NodeHostnameToId(nodeHostname));
                retVal += "-> Node: " + node.getHostname() + " " + node.getId() + "\n";
                retVal += "--> Avail Resources: {Mem " + node.getAvailableMemoryResources() + ", CPU " + node.getAvailableCpuResources() + " Slots: " + node.totalSlotsFree() + "}\n";
                retVal += "--> Total Resources: {Mem " + node.getTotalMemoryResources() + ", CPU " + node.getTotalCpuResources() + " Slots: " + node.totalSlots() + "}\n";
            }
        }
        return retVal;
    }

    /**
     * hostname to Id
     * @param hostname
     * @return the id of a node
     */
    public String NodeHostnameToId(String hostname) {
        for (RAS_Node n : _nodes.getNodes()) {
            if (n.getHostname() == null) {
                continue;
            }
            if (n.getHostname().equals(hostname)) {
                return n.getId();
            }
        }
        LOG.error("Cannot find Node with hostname {}", hostname);
        return null;
    }

    /**
     * Find RAS_Node for specified node id
     * @param id
     * @return a RAS_Node object
     */
    public RAS_Node idToNode(String id) {
        RAS_Node ret = _nodes.getNodeById(id);
        if(ret == null) {
            LOG.error("Cannot find Node with Id: {}", id);
        }
        return ret;
    }
}
