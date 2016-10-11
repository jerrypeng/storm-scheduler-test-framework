package org.apache.storm.scheduler.performance;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SchedulerAssignmentImpl;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.RAS_Nodes;
import org.apache.storm.scheduler.resource.strategies.scheduling.IStrategy;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jerrypeng on 8/2/16.
 */
public class PerformanceTestSuite {
    private Cluster cluster;

    private List<Class<? extends IStrategy>> strategies = new LinkedList<>();

    private Map<String, TopologyDetails> testTopologies = new HashMap<>();

    private Map defaultConfigs = new HashMap();

    private JsonGenerator jsonGenerator;

    //file to write data to
    private final String filename;

    private AtomicInteger sessionId = new AtomicInteger(0);

    private static final Logger LOG = LoggerFactory.getLogger(PerformanceTestSuite.class);

    public PerformanceTestSuite(Map defaultConfigs, String filename) {
        this.filename = filename;
        this.defaultConfigs.putAll(defaultConfigs);
        JsonFactory jfactory = new JsonFactory();
        File file = new File(this.filename);
        if (!file.exists()) {
            try {
                //create directories if needed
                file.getParentFile().mkdirs();
                //json write
                this.jsonGenerator = jfactory.createJsonGenerator(file, JsonEncoding.UTF8);
                this.jsonGenerator.writeStartArray();
            } catch (IOException e) {
                LOG.error("{}", e);
            }
        } else {
            throw new RuntimeException(filename + " already exists!");
        }
    }

    public void buildTestCluster(int numSupervisors, int numWorkersPerSup, Map clusterConfs) {
        /* Building test cluster */
        //generate supervisors
        Map<String, SupervisorDetails> supMap = PerformanceUtils.genSupervisors(numSupervisors, numWorkersPerSup, clusterConfs);
        buildTestCluster(supMap, clusterConfs);
    }

    public void buildTestCluster(Map<String, SupervisorDetails> supMap, Map clusterConfs) {
        this.defaultConfigs.putAll(clusterConfs);
        this.cluster = new Cluster(new PerformanceUtils.INimbusTest(), supMap, new HashMap<String, SchedulerAssignmentImpl>(), this.defaultConfigs);
    }

    public void buildTestCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public void addTestTopology(TopologyDetails topo) {
        this.testTopologies.put(topo.getId(), topo);
    }

    public void clearTestTopologies() {
        this.testTopologies.clear();
    }

    public void addSchedulingStrategyToTest(Class<? extends IStrategy> clazz) {
        this.strategies.add(clazz);
    }

    public void run() {

        if (this.strategies.isEmpty()) {
            LOG.warn("No Strategies specified!");
            return;
        }
        if (this.cluster == null) {
            LOG.warn("No cluster has been built!");
            return;
        }

        for (Map.Entry<String, TopologyDetails> entry : this.testTopologies.entrySet()) {
            TopologyDetails topo = entry.getValue();
            Map<String, TopologyDetails> topologyDetailsMap = new HashMap<String, TopologyDetails>();
            topologyDetailsMap.put(entry.getKey(), entry.getValue());

            LOG.info("!---- TestSession {} ----!", this.sessionId);
            /* debug */
            LOG.info("/** Topology {} **/", topo.getName());
            for (ExecutorDetails exec : topo.getExecutors()) {
                LOG.info("Exec: {} Mem: {} CPU: {}", exec, topo.getTotalMemReqTask(exec), topo.getTotalCpuReqTask(exec));
            }
            LOG.info("{}", topo.getComponents());

            LOG.info("/** Cluster **/");
            LOG.info("{}", RAS_Nodes.getAllNodesFrom(this.cluster, new Topologies(this.testTopologies)));

            /* debug end */
            if (this.sessionId.get() % 1000 == 0) {
                System.out.println("!---- TestSession " + this.sessionId + "----!");
            }
            PerformanceTestSession testSession = new PerformanceTestSession(this.cluster, new Topologies(topologyDetailsMap), this.strategies, this.defaultConfigs, this.sessionId.get());
            try {
                testSession.run();
            } catch (Exception e ) {
                LOG.error("{}", e);
                throw new RuntimeException(e);
            }
            //LOG.info(testSession.toString());
            writeJson(testSession.toJsonObject());
            this.sessionId.getAndIncrement();
        }
    }

    public void endTest() {
        try {
            this.jsonGenerator.writeEndArray();
            this.jsonGenerator.close();
        } catch (IOException e) {
            LOG.error("{}", e);
        }
    }

    private synchronized void writeJson(JSONObject object) {

        try {

            this.jsonGenerator.writeStartObject();
            this.jsonGenerator.writeStringField(PerformanceTestSession.Config.testSessionId, object.get(PerformanceTestSession.Config.testSessionId).toString());

            this.jsonGenerator.writeFieldName(PerformanceTestSession.Config.performanceResults);
            this.jsonGenerator.writeRawValue(((JSONObject) object.get(PerformanceTestSession.Config.performanceResults)).toJSONString());
            this.jsonGenerator.writeEndObject();


        } catch (IOException e) {
            LOG.error("{}", e);
            throw new RuntimeException(e);
        }
    }

    public void createTestReport(String resultsPath) {

        PerformanceTestReport report = new PerformanceTestReport();
        report.addDataFile(this.filename);

        report.generateReport(resultsPath);
    }

    public int getSessionId() {
        return sessionId.get();
    }
}
