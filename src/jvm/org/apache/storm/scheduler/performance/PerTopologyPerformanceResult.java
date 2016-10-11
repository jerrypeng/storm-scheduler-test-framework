package org.apache.storm.scheduler.performance;

import org.apache.storm.scheduler.resource.SchedulingResult;
import org.json.simple.JSONObject;

/**
 * Created by jerrypeng on 8/12/16.
 */
public class PerTopologyPerformanceResult {
    private Double networkClosenessMetric = null;
    private long timeToRun;
    private SchedulingResult result;
    private String topoId;
    private String printableStats;

    public static final class Config {
        public static final String networkClosenessMetric = "networkClosenessMetric";
        public static final String timeToRun = "timeToRun";
        public static final String SchedulingResult = "SchedulingResult";
        public static final String topoId = "topoId";
        public static final String SchedulingSuccess = "SchedulingSuccess";
        public static final String printableStats = "printableStats";
    }

    @Override
    public String toString() {
        JSONObject object = new JSONObject();
        object.put(Config.networkClosenessMetric, this.networkClosenessMetric);
        object.put(Config.timeToRun, timeToRun);
        object.put(Config.SchedulingResult, this.result.toString());
        object.put(Config.topoId, this.topoId);
        object.put(Config.SchedulingSuccess, this.result.isSuccess());
        return object.toJSONString() + "\n" + this.printableStats;
    }

    public JSONObject toJsonObject() {
        JSONObject object = new JSONObject();

        object.put(Config.networkClosenessMetric, this.networkClosenessMetric);
        object.put(Config.timeToRun, timeToRun);
        object.put(Config.SchedulingResult, this.result.toString());
        object.put(Config.topoId, this.topoId);
        object.put(Config.printableStats, this.printableStats);
        object.put(Config.SchedulingSuccess, this.result.isSuccess());
        return object;
    }

    public Double getNetworkClosenessMetric() {
        return networkClosenessMetric;
    }

    public void setNetworkClosenessMetric(Double networkClosenessMetric) {
        this.networkClosenessMetric = networkClosenessMetric;
    }

    public long getTimeToRun() {
        return timeToRun;
    }

    public void setTimeToRun(long timeToRun) {
        this.timeToRun = timeToRun;
    }

    public SchedulingResult getResult() {
        return result;
    }

    public void setResult(SchedulingResult result) {
        this.result = result;
    }

    public String getTopoId() {
        return topoId;
    }

    public void setTopoId(String topoId) {
        this.topoId = topoId;
    }

    public String getPrintableStats() {
        return printableStats;
    }

    public void setPrintableStats(String printableStats) {
        this.printableStats = printableStats;
    }
}
