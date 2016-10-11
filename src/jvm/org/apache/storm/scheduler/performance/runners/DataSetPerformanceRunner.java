package org.apache.storm.scheduler.performance.runners;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.SchedulerAssignmentImpl;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.strategies.scheduling.IStrategy;
import org.apache.storm.utils.Utils;

import org.apache.storm.scheduler.performance.PerformanceUtils;
import org.apache.storm.scheduler.performance.random.GenRandomCluster;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jerrypeng on 9/14/16.
 */
public class DataSetPerformanceRunner extends MultithreadedStrategyPerformanceRunner{

    private List<File> stormTopologyFiles = new LinkedList<File>();
    private Map<String, File> topologyConfFiles = new HashMap<String, File>();

    private static final Logger LOG = LoggerFactory.getLogger(DataSetPerformanceRunner.class);

    static class RunThread extends MultithreadedStrategyPerformanceRunner.RunThread {

        public RunThread(int iterations, Config defautConfs, String filename, Collection<Class<? extends IStrategy>> strategies, Object args) {
            super(iterations, defautConfs, filename, strategies, args);
        }

        @Override
        public void run() {
            List<TopologyDetails> testTopos = (List<TopologyDetails>) this.args;
            int totalCount = testTopos.size() * this.iterations;
            int count = 0;
            for (int i = 0; i < this.iterations; i++) {
                for (TopologyDetails topo : testTopos) {
                    System.out.println("[Thread-" + Thread.currentThread().getId() + "] Completed " + count + " / " + totalCount + " : " + topo.getId() + " - Session: " + perf.getSessionId());
                    perf.addTestTopology(topo);
                    Cluster cluster = generateClusterForTopo(topo);
                    perf.buildTestCluster(cluster);
                    perf.run();
                    perf.clearTestTopologies();
                    count++;
                }
            }
            this.perf.endTest();
        }

        private Cluster generateClusterForTopo(TopologyDetails topo) {
            double totalMem = topo.getTotalRequestedMemOffHeap() + topo.getTotalRequestedMemOnHeap();
            double totalCpu = topo.getTotalRequestedCpu();

            int numSups = 10;
            int portsPerSup = 20;
            int numRacks=3;

//
//        Map resourceMap = new HashMap();
//        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, (totalMem / 2));
//        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, (totalCpu / 2 ));
//        Map<String, SupervisorDetails> supMap = TestUtilsForResourceAwareScheduler.genSupervisors(numSups, portsPerSup, resourceMap);
//        Cluster cluster = new Cluster(new TestUtilsForResourceAwareScheduler.INimbusTest(), supMap, new HashMap<String, SchedulerAssignmentImpl>(), this.defaultConfs);

            INimbus iNimbus = new PerformanceUtils.INimbusTest();

            List<String> racks = GenRandomCluster.generateRandomRacks(numRacks);
            Map<String, SupervisorDetails> sups = new HashMap<>();
            for (int i = 0; i<numSups; i++) {
               SupervisorDetails sup = GenRandomCluster.genRandomSupervisor(portsPerSup, totalCpu * 0.5, totalMem * 0.5, portsPerSup / 2, totalCpu * 0.2, totalMem * 0.2);
                sups.put(sup.getId(), sup);
            }

            Map<String, List<String>> supervisorToRacks = GenRandomCluster.assignSupervisorsToRacks(sups, racks);

            Cluster cluster = new Cluster(iNimbus, sups, new HashMap<String, SchedulerAssignmentImpl>(), this.defaultConfs);
            cluster.setNetworkTopography(supervisorToRacks);
            return cluster;

//            GenRandomCluster randomCluster = new GenRandomCluster(this.defaultConfs);
//            return randomCluster.generateRandomCluster(10, 20, 3, totalCpu * 8, totalMem *8);
            //   return cluster;
        }
    }

    public DataSetPerformanceRunner(int numThreads, int numIterations, Config defaultConfs, String dataPath) {
        super(numThreads, numIterations, defaultConfs, dataPath, RunThread.class);
    }

    public void addStormTopologyDataFile(String filename) {
        File file = new File(filename);
        this.stormTopologyFiles.add(file);
    }

    public void addStormTopologyDataDir(String dir) {
        File folder = new File(dir);
        if (folder.exists() && folder.isDirectory()) {
            File[] listOfFiles = folder.listFiles();
            for (File file : listOfFiles) {
                if (file.isFile() && !file.isHidden()) {
                    this.stormTopologyFiles.add(file);
                }
            }
        } else {
            LOG.warn("{} is not a directory!", dir);
        }
    }

    public void addTopologyConfDataFile(String filename) {
        File file = new File(filename);

        this.topologyConfFiles.put(filename.replaceAll(".ser", ""), file);
    }

    public void addTopologyConfDataDir(String dir) {
        File folder = new File(dir);
        if (folder.exists() && folder.isDirectory()) {
            File[] listOfFiles = folder.listFiles();
            for (File file : listOfFiles) {
                if (file.isFile() && !file.isHidden()) {
                    this.topologyConfFiles.put(file.getName().replaceAll(".ser", ""), file);
                }
            }
        } else {
            LOG.warn("{} is not a directory!", dir);
        }
    }

    @Override
    Object getRunArgs() {
        Object args;
        try {
            args = readDataSet();
        } catch (IOException | ClassNotFoundException | ParseException ex) {
            throw new RuntimeException(ex);
        }
        return args;
    }

    public List<TopologyDetails> readDataSet() throws IOException, ClassNotFoundException, ParseException {
        List<TopologyDetails> topos = new LinkedList<TopologyDetails>();
        if (this.stormTopologyFiles.isEmpty()) {
            LOG.warn("No Storm topologies found!");
        }
        if (this.topologyConfFiles.isEmpty()) {
            LOG.warn("No topology confs found!");
        }

        for (File file : this.stormTopologyFiles) {
            String id = file.getName().replaceAll(".ser", "");

            LOG.debug("id: {} file: {}", id, file.getAbsoluteFile());
            StormTopology stormTopology = (StormTopology) deserializeStormTopologyObject(file);

            if (this.topologyConfFiles.containsKey(id)) {
                Map topologyConf = new HashMap();
                //in case default confs are not included
                topologyConf.putAll(this.defaultConfs);
                topologyConf.putAll((Map) new JSONParser().parse((String) deserializeTopologyConfObject(this.topologyConfFiles.get(id))));


                TopologyDetails topo = createTopologyDetailsObject(file.getName(), stormTopology, topologyConf);
                topos.add(topo);
            } else {
                LOG.warn("Topology conf for {} not found! Skipping this topology", id);
            }
        }
        LOG.info("{} topologies created", topos.size());
        return topos;
    }

    public Object deserializeStormTopologyObject(File file) throws IOException, ClassNotFoundException {

        byte[] bytes = Files.readAllBytes(file.toPath());
        return Utils.thriftDeserialize(StormTopology.class, bytes);
    }

    public Object deserializeTopologyConfObject(File file) throws IOException {
        byte[] bytes = Files.readAllBytes(file.toPath());
        return Utils.javaDeserialize(bytes, String.class);
    }

    public TopologyDetails createTopologyDetailsObject(String topoId, StormTopology stormTopology, Map topologyConf) {
        return new TopologyDetails(topoId, topologyConf, stormTopology, 0, PerformanceUtils.genExecsAndComps(stormTopology), 0);
    }
}
