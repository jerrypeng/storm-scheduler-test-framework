package org.apache.storm.scheduler.performance.random;

import org.apache.storm.Config;
import org.apache.storm.scheduler.performance.PerformanceUtils;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.SchedulerAssignmentImpl;
import org.apache.storm.scheduler.SupervisorDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Created by jerrypeng on 8/4/16.
 */

public class GenRandomCluster {

    private static final Logger LOG = LoggerFactory.getLogger(GenRandomCluster.class);

    private Config defaultConfs = new Config();


    private static final int PORT_RANGE_START = 6000;

    public GenRandomCluster(Config defaultConfs) {
        this.defaultConfs.putAll(defaultConfs);
    }

    public Cluster generateRandomCluster(int maxSupervisors, int maxPortsPerSupervisor, int maxRacks, double maxCpu, double maxMem) {

        INimbus iNimbus = new PerformanceUtils.INimbusTest();

        List<String> racks = generateRandomRacks(maxRacks);
        Map<String, SupervisorDetails> sups = genRandomSupervisors(maxSupervisors, maxPortsPerSupervisor, maxCpu, maxMem);

        Map<String, List<String>> supervisorToRacks = assignSupervisorsToRacks(sups, racks);

        Cluster cluster = new Cluster(iNimbus, sups, new HashMap<String, SchedulerAssignmentImpl>(), this.defaultConfs);
        cluster.setNetworkTopography(supervisorToRacks);

        return cluster;
    }

    public static Map<String, List<String>> assignSupervisorsToRacks(Map<String, SupervisorDetails> sups, List<String> racks) {
        Map<String, List<String>> ret = new HashMap<String, List<String>>();
        for (String rackId : racks) {
            ret.put(rackId, new LinkedList<String>());
        }
        Random random = new Random();
        int numRacks = racks.size();
        for (SupervisorDetails sup : sups.values()) {
            int rackIndex = PerformanceUtils.getRandomIntInRange(0, numRacks, random);
            String rackId = racks.get(rackIndex);
            ret.get(rackId).add(sup.getHost());
        }
        return ret;
    }

    public static List<String> generateRandomRacks(int maxRacks) {
        int numRacks = PerformanceUtils.getRandomIntInRange(1, maxRacks + 1, new Random());
        List<String> ret = new ArrayList<String>(numRacks);
        for (int i = 0; i < numRacks; i++) {
            ret.add("rack-" + i);
        }
        return ret;
    }

    public static Map<String, SupervisorDetails> genRandomSupervisors(int maxSupervisors, int maxPorts, double maxCpu, double maxMem) {

        Random random = new Random();

        int availSupervisors = PerformanceUtils.getRandomIntInRange(1, maxSupervisors + 1, random);
        int availPorts = PerformanceUtils.getRandomIntInRange(0, maxPorts + 1, random);
        double availCpu = PerformanceUtils.getRandomIntInRange(0, (int) maxCpu + 1, random);
        double availMem = PerformanceUtils.getRandomIntInRange(0, (int) maxMem + 1, random);

        Map<String, SupervisorDetails> sups = new HashMap<String, SupervisorDetails>();

        while ((availSupervisors > 0) && (availPorts > 0 ) && (availCpu > 0.0) && (availMem > 0.0)) {

            SupervisorDetails sup = genRandomSupervisor(availPorts, availCpu, availMem);

            //consume allowed resources for sup
            availSupervisors--;
            availCpu = availCpu - sup.getTotalCPU();
            availMem = availMem - sup.getTotalMemory();
            availPorts = availPorts - sup.getAllPorts().size();

            sups.put(sup.getId(), sup);
        }

        return sups;
    }

    public static  SupervisorDetails genRandomSupervisor(int maxPorts, double maxCpu, double maxMem) {
       return genRandomSupervisor(maxPorts, maxCpu, maxMem, 0, 0, 0);
    }

    public static  SupervisorDetails genRandomSupervisor(int maxPorts, double maxCpu, double maxMem, int minPorts, double minCpu, double minMem) {

        Random random = new Random();

        //generate random number of ports
        int numPort = PerformanceUtils.getRandomIntInRange(minPorts, maxPorts + 1, random);
        Collection<Number> ports = new LinkedList<Number>();
        for (int i=0; i< numPort; i++) {
            ports.add(PORT_RANGE_START + i);
        }

        Map<String, Double> resources = new HashMap<String, Double>();

        //generate random amount of cpu
        double cpu = PerformanceUtils.getRandomIntInRange((int) minCpu, (int) maxCpu + 1, random);
        resources.put(Config.SUPERVISOR_CPU_CAPACITY, cpu);

        //generate random amount of memory
        double mem = PerformanceUtils.getRandomIntInRange((int) minMem, (int) maxMem + 1, random);
        resources.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, mem);

        UUID id = UUID.randomUUID();

        return new SupervisorDetails("host-" + id.toString(), "id" + id.toString(), null, null, ports, resources);
    }

}
