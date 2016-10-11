package org.apache.storm.scheduler.performance;

import org.apache.storm.utils.Utils;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jerrypeng on 8/12/16.
 */
public class PerformanceTestReport {

    private List<JsonParser> jsonParsers = new LinkedList<JsonParser>();
    private DescriptiveStatistics overallStats = new DescriptiveStatistics();
    private Map<String, DescriptiveStatistics> networkMetricsStats = new HashMap<String, DescriptiveStatistics>();
    private Map<String, SummaryStatistics> runtimeStats = new HashMap<String, SummaryStatistics>();

    //strategy -> fail?succeed in scheduling -> other strategy -> number of suceed or failure
    private Map<String, Map<String ,Map<String, Integer>>> successMap = new HashMap<String, Map<String, Map<String, Integer>>>();

    private static final Logger LOG = LoggerFactory.getLogger(PerformanceTestReport.class);

    public PerformanceTestReport () {
        this(null);
    }
    public PerformanceTestReport(String dataSrcPath) {
        if (dataSrcPath != null) {
            addDataFileDirectory(dataSrcPath);
        }
    }

    public void addDataFile(String filePath) {
        File file = new File(filePath);
        addFileForParsing(file);
    }

    public void addDataFileDirectory(String dirPath) {
        File folder = new File(dirPath);
        if (folder.exists() && folder.isDirectory()) {
            File[] listOfFiles = folder.listFiles();
            for (File file : listOfFiles) {
                addFileForParsing(file);
            }
        } else {
            LOG.warn("{} is not a directory!", dirPath);
        }
    }

    private void addFileForParsing(File file) {
        if (file.isFile() && !file.isHidden()) {
            try {
                //json read
                ObjectMapper mapper = new ObjectMapper();
                JsonFactory jsonFactory = mapper.getJsonFactory();
                this.jsonParsers.add(jsonFactory.createJsonParser(new FileInputStream(file)));
            } catch (IOException e) {
                LOG.error("{}", e);
            }
        }
    }

    public void generateReport() {
        this.generateReport("results.json");
    }

    public void generateReport(String resultsFilename) {

        if (this.jsonParsers.isEmpty()) {
            LOG.error("Did not add any data files to parse!");
            return;
        }

        int numResults = 0;
        try {
            while (true) {
                JSONObject object = getJsonObject();
                if (object == null) {
                    break;
                }

                JSONObject performanceResultsObjects  = (JSONObject) object.get(PerformanceTestSession.Config.performanceResults);
                //LOG.info("{}", performanceResultsObjects.toJSONString());

                populateSchedulingSuccessStats((Map<String, JSONObject>) performanceResultsObjects);

                //skip if not all topologies are scheduled successful
                if (!schedulingSuccess((Map<String, JSONObject>) performanceResultsObjects)) {
                    continue;
                } else {
                    numResults ++;
                    populateNetworkSumStats((Map<String, JSONObject>) performanceResultsObjects);
                }
            }

            LOG.info("successMap: {}", this.successMap);

            for (Map.Entry<String, DescriptiveStatistics> entry : this.networkMetricsStats.entrySet()) {
                LOG.info("Strategy: {} Sum: {} avg: {}", entry.getKey(), entry.getValue().getSum(), entry.getValue().getMean());
            }
            writeResults(resultsFilename);

        } catch (IOException | ParseException e) {
            LOG.error("{}", e);
            throw new RuntimeException(e);
        }
    }

    public void populateSchedulingSuccessStats(Map<String, JSONObject> performanceResultsObjects) {


        for (Map.Entry<String, JSONObject> entry : (performanceResultsObjects).entrySet()) {
            String strategyUsed = entry.getKey();
            JSONObject performanceResultsObject = entry.getValue();
            String status = (Utils.getInt(performanceResultsObject.get(PerformanceResult.Config.totalNumNodesUsed)) > 0) ? "success" : "failure";
            if (!this.successMap.containsKey(strategyUsed)) {
                this.successMap.put(strategyUsed, new HashMap<String, Map<String, Integer>>());
            }
            if (!this.successMap.get(strategyUsed).containsKey(status)) {
                this.successMap.get(strategyUsed).put(status, new HashMap<String, Integer>());
            }

            Map<String, Integer> map = this.successMap.get(strategyUsed).get(status);

            for (Map.Entry<String, JSONObject> entry2 : (performanceResultsObjects).entrySet()) {
                String strategy = entry2.getKey();
                JSONObject performanceResultsObject2 = entry2.getValue();

                if(!map.containsKey(strategy)) {
                    map.put(strategy, 0);
                }
                String status2 = (Utils.getInt(performanceResultsObject2.get(PerformanceResult.Config.totalNumNodesUsed)) > 0) ? "success" : "failure";
                if (status.equals(status2)) {
                    map.put(strategy, map.get(strategy) + 1);
                }
            }
        }
    }


    public void populateNetworkSumStats(Map<String, JSONObject> performanceResultsObjects) {
        for (Map.Entry<String, JSONObject> entry : (performanceResultsObjects).entrySet()) {
            String strategyUsed = entry.getKey();
            JSONObject performanceResultsObject = entry.getValue();

            if (performanceResultsObject.get(PerformanceResult.Config.avgNetworkClosenessMetric) != null) {
                double avgNetworkClosenessMetric = (double) performanceResultsObject.get(PerformanceResult.Config.avgNetworkClosenessMetric);

                if (!this.networkMetricsStats.containsKey(strategyUsed)) {

                    this.networkMetricsStats.put(strategyUsed, new DescriptiveStatistics());
                }

                this.networkMetricsStats.get(strategyUsed).addValue(avgNetworkClosenessMetric);
            }

            if (performanceResultsObject.get(PerformanceResult.Config.avgTimeToRun) != null) {

                long avgRuntime = (long) performanceResultsObject.get(PerformanceResult.Config.avgTimeToRun);
                if (!this.runtimeStats.containsKey(strategyUsed)) {
                    this.runtimeStats.put(strategyUsed, new SummaryStatistics());
                }

                this.runtimeStats.get(strategyUsed).addValue(avgRuntime);
            }
        }
    }

    public void writeResults(String filename) throws FileNotFoundException, UnsupportedEncodingException {
        JSONObject jsonObject = new JSONObject();

        JSONObject networkStats = new JSONObject();
        JSONObject timeStats = new JSONObject();


        for (Map.Entry<String, DescriptiveStatistics> entry : this.networkMetricsStats.entrySet()) {
            String strategy = entry.getKey();
            DescriptiveStatistics stats = entry.getValue();
            JSONObject tmpObject = new JSONObject();
            tmpObject.put("sum", stats.getSum());
            tmpObject.put("avg", stats.getMean());
            tmpObject.put("std", stats.getStandardDeviation());
            tmpObject.put("max", stats.getMax());
            Collection<Double> list = new LinkedList<Double>();
            for (int i = 1; i <= 100; i++) {
                list.add(stats.getPercentile(i));
            }
            tmpObject.put("percentiles", new JSONArray(list));

            networkStats.put(strategy, tmpObject);
        }
        jsonObject.put("networkMetricStats", networkStats);


        for (Map.Entry<String, SummaryStatistics> entry : this.runtimeStats.entrySet()) {
            String strategy = entry.getKey();
            SummaryStatistics stats = entry.getValue();
            JSONObject tmpObject = new JSONObject();
            tmpObject.put("avg", stats.getMean());
            tmpObject.put("std", stats.getStandardDeviation());
            tmpObject.put("max", stats.getMax());
            timeStats.put(strategy, tmpObject);
        }
        jsonObject.put("runtimeStats", timeStats);

        JSONObject schedulingStatusStats = new JSONObject();
        schedulingStatusStats.putAll(this.successMap);

        jsonObject.put("schedulingStatusStats", schedulingStatusStats);

        PrintWriter writer = new PrintWriter(filename, "UTF-8");

        writer.println(jsonObject.toJSONString());

        writer.close();
    }


    private boolean schedulingSuccess(Map<String, JSONObject> resultsObject) {
        for (JSONObject entry : resultsObject.values()) {
            if (Utils.getInt(entry.get(PerformanceResult.Config.totalNumNodesUsed)) <= 0) {
                return false;
            }
        }
        return true;
    }


    private JSONObject getJsonObject() throws IOException, ParseException {

        JSONObject ret = null;
        String json = parseJsonObject();
        if (json != null) {
            ret =  (JSONObject) (new JSONParser()).parse(json);
        }
        return ret;
    }


    private int parserIndex=0;
    private String parseJsonObject() throws IOException {

       while (this.parserIndex < this.jsonParsers.size()) {

           JsonParser jsonParser = this.jsonParsers.get(this.parserIndex);

           JsonToken token;
           while ((token = jsonParser.nextToken()) != null) {
               if (token == JsonToken.START_OBJECT) {
                   JsonNode node = jsonParser.readValueAsTree();
                   return node.toString();
               }
           }
           this.parserIndex++;
       }
        return null;
    }
}
