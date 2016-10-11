package org.apache.storm.scheduler.performance;

import org.apache.storm.scheduler.resource.SchedulingState;
import org.apache.storm.scheduler.resource.strategies.scheduling.IStrategy;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jerrypeng on 8/1/16.
 */

/**
 * Performance result for a scheduling strategy in a performance test session
 */
public class PerformanceResult {

    private static final Logger LOG = LoggerFactory.getLogger(PerformanceResult.class);

    //TopologyId -> PerTopologyPerformanceResult
    private Map<String, PerTopologyPerformanceResult> perTopologyPerformanceResultMap = new ConcurrentHashMap<>();
    private Class<? extends IStrategy> strategyUsed;
    private String statsToPrint;
    private int totalNumNodesUsed;
    private Double avgCpuUtilization = null;
    private Double avgMemoryUtilization = null;
    private long avgTimeToRun;
    private Double avgNetworkClosenessMetric = null;
    private SchedulingState schedulingState;

    public static final class Config {
        public static final String strategyUsed = "strategyUsed";
        public static final String totalNumNodesUsed = "totalNumNodesUsed";
        public static final String avgCpuUtilization = "avgCpuUtilization";
        public static final String avgMemoryUtilization = "avgMemoryUtilization";
        public static final String avgTimeToRun = "avgTimeToRun";
        public static final String avgNetworkClosenessMetric = "avgNetworkClosenessMetric";
        public static final String schedulingState = "schedulingState";
        public static final String statsToPrint = "statsToPrint";
        public static final String perTopologyPerformanceResults = "perTopologyPerformanceResults";
    }

    @Override
    public String toString() {
        JSONObject object = new JSONObject();
        object.put(Config.strategyUsed, this.strategyUsed.getCanonicalName());
        object.put(Config.totalNumNodesUsed, this.totalNumNodesUsed);
        object.put(Config.avgCpuUtilization, this.avgCpuUtilization);
        object.put(Config.avgMemoryUtilization, this.avgMemoryUtilization);
        object.put(Config.avgTimeToRun,this.avgTimeToRun);
        object.put(Config.avgNetworkClosenessMetric, this.avgNetworkClosenessMetric);
        object.put(Config.schedulingState, this.schedulingState);
        return object.toJSONString() + "\n" + this.statsToPrint;
    }

    public JSONObject toJsonObject() {
        JSONObject object = new JSONObject();
        object.put(Config.strategyUsed, this.strategyUsed.getCanonicalName());
        object.put(Config.totalNumNodesUsed, this.totalNumNodesUsed);
        object.put(Config.avgCpuUtilization, this.avgCpuUtilization);
        object.put(Config.avgMemoryUtilization, this.avgMemoryUtilization);
        object.put(Config.avgTimeToRun,this.avgTimeToRun);
        object.put(Config.avgNetworkClosenessMetric, this.avgNetworkClosenessMetric);
        object.put(Config.statsToPrint, this.statsToPrint);
        JSONObject tmpObject = new JSONObject();
        for (Map.Entry<String, PerTopologyPerformanceResult> entry : this.perTopologyPerformanceResultMap.entrySet()) {
            String topoId = entry.getKey();
            tmpObject.put(topoId, entry.getValue().toJsonObject());
        }
        object.put(Config.perTopologyPerformanceResults, tmpObject);
        return object;
    }

    public Map<String, PerTopologyPerformanceResult> getPerTopologyPerformanceResultMap() {
        return perTopologyPerformanceResultMap;
    }

    public void setPerTopologyPerformanceResultMap(Map<String, PerTopologyPerformanceResult> perTopologyPerformanceResultMap) {
        this.perTopologyPerformanceResultMap = perTopologyPerformanceResultMap;
    }

    public SchedulingState getSchedulingState() {
        return schedulingState;
    }

    public void setSchedulingState(SchedulingState schedulingState) {
        this.schedulingState = schedulingState;
    }


    public Class<? extends IStrategy> getStrategyUsed() {
        return strategyUsed;
    }

    public void setStrategyUsed(Class<? extends IStrategy> strategyUsed) {
        this.strategyUsed = strategyUsed;
    }

    public String getStatsToPrint() {
        return statsToPrint;
    }

    public void setStatsToPrint(String statsToPrint) {
        this.statsToPrint = statsToPrint;
    }

    public int getTotalNumNodesUsed() {
        return totalNumNodesUsed;
    }

    public void setTotalNumNodesUsed(int totalNumNodesUsed) {
        this.totalNumNodesUsed = totalNumNodesUsed;
    }

    public Double getAvgCpuUtilization() {
        return avgCpuUtilization;
    }

    public void setAvgCpuUtilization(Double avgCpuUtilization) {
        this.avgCpuUtilization = avgCpuUtilization;
    }

    public Double getAvgMemoryUtilization() {
        return avgMemoryUtilization;
    }

    public void setAvgMemoryUtilization(Double avgMemoryUtilization) {
        this.avgMemoryUtilization = avgMemoryUtilization;
    }

    public long getAvgTimeToRun() {
        return avgTimeToRun;
    }

    public void setAvgTimeToRun(long avgTimeToRun) {
        this.avgTimeToRun = avgTimeToRun;
    }

    public Double getAvgNetworkClosenessMetric() {
        return avgNetworkClosenessMetric;
    }

    public void setAvgNetworkClosenessMetric(Double avgNetworkClosenessMetric) {
        this.avgNetworkClosenessMetric = avgNetworkClosenessMetric;
    }
}
