package org.apache.storm.scheduler.performance.random;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.performance.PerformanceUtils;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Created by jerrypeng on 8/4/16.
 */
public class GenRandomTopology {

    private Config defaultConfs = new Config();

    private static final Logger LOG = LoggerFactory.getLogger(GenRandomTopology.class);


    public GenRandomTopology(Config defaultConfs) {
        this.defaultConfs.putAll(defaultConfs);
    }

    public TopologyDetails generateRandomTopology(int maxExecutors, double maxCpu, double maxMem) {

        Random random = new Random();

        int availExecutors = PerformanceUtils.getRandomIntInRange(1, maxExecutors + 1, random);
        double availCpu = PerformanceUtils.getRandomIntInRange(1, (int)maxCpu + 1, random);
        double availMem = PerformanceUtils.getRandomIntInRange(1, (int)maxMem + 1, random);

        TopologyBuilder builder = new TopologyBuilder();

        int index = 0;
        int numSpout = 0;
        int numBolt = 0;
        List<SpoutDeclarer> spouts = new LinkedList<SpoutDeclarer>();
        List<BoltDeclarer> bolts = new LinkedList<BoltDeclarer>();
        while (availExecutors > 0 && availCpu > 0.0 && availMem > 0.0) {
            if (index % 2 == 0) {
                PerformanceUtils.Triple<SpoutDeclarer, Double, Double> triple = generateRandomSpout(availExecutors, availCpu, availMem, builder, "spout-" + numSpout);
                SpoutDeclarer spoutDeclarer = triple.x;
                spouts.add(spoutDeclarer);
                availCpu = availCpu - triple.y;
                availMem = availMem - triple.z;
                numSpout++;
                availExecutors--;
            } else {
                PerformanceUtils.Triple<BoltDeclarer, Double, Double> triple = generateRandomBolt(availExecutors, availCpu, availMem, builder, "bolt-" + numBolt);
                BoltDeclarer boltDeclarer = triple.x;
                bolts.add(boltDeclarer);
                availCpu = availCpu - triple.y;
                availMem = availMem - triple.z;
                numBolt++;
                availExecutors--;
            }
            index++;
        }


        //connect spouts
        if (numBolt > 0) {
            for (int i = 0; i < spouts.size(); i++) {
                int k = PerformanceUtils.getRandomIntInRange(0, numBolt, random);
                bolts.get(k).shuffleGrouping("spout-" + i);
            }
        }

        //connect bolts
        if (numBolt > 1) {
            for (int i = 0; i < bolts.size(); i++) {
                int k = 0;
                // not connect to itself
                while (true) {
                    k = PerformanceUtils.getRandomIntInRange(0, numBolt, random);
                    if (k != i) {
                        break;
                    }
                }
                bolts.get(k).shuffleGrouping("bolt-" + i);
            }
        }

        StormTopology stormTopology = builder.createTopology();

        String uuid = UUID.randomUUID().toString();
        this.defaultConfs.put(Config.TOPOLOGY_NAME, "topoName-" + uuid);

        TopologyDetails topo = new TopologyDetails("topoId-" + uuid, this.defaultConfs, stormTopology, 0, PerformanceUtils.genExecsAndComps(stormTopology), 0);
        return topo;
    }


    public PerformanceUtils.Triple<SpoutDeclarer, Double, Double> generateRandomSpout(int maxExecutors, double maxCpu, double maxMem, TopologyBuilder builder, String id) {
        Random random = new Random();

        //generate random number of executors
        int numExecutors = PerformanceUtils.getRandomIntInRange(1, maxExecutors + 1, random);

        //generate random amount of cpu
        double cpu = PerformanceUtils.getRandomIntInRange(1, (int) maxCpu + 1, random);

        //generate random amount of memory
        double mem = PerformanceUtils.getRandomIntInRange(1, (int) maxMem + 1, random);

        SpoutDeclarer spoutDeclarer = builder.setSpout(id, new TestSpout(), numExecutors).setCPULoad(cpu).setMemoryLoad(mem);
        return new PerformanceUtils.Triple(spoutDeclarer, cpu, mem);
    }

    public PerformanceUtils.Triple<BoltDeclarer, Double, Double> generateRandomBolt(int maxExecutors, double maxCpu, double maxMem, TopologyBuilder builder, String id) {
        Random random = new Random();

        //generate random number of executors
        int numExecutors = PerformanceUtils.getRandomIntInRange(1, maxExecutors + 1, random);

        //generate random amount of cpu
        double cpu = PerformanceUtils.getRandomIntInRange(1, (int) maxCpu + 1, random);

        //generate random amount of memory
        double mem = PerformanceUtils.getRandomIntInRange(1, (int) maxMem + 1, random);

        BoltDeclarer boltDeclarer = builder.setBolt(id, new TestBolt(), numExecutors).setCPULoad(cpu).setMemoryLoad(mem);

        return new PerformanceUtils.Triple<BoltDeclarer, Double, Double>(boltDeclarer, cpu, mem);
    }


    public static class TestBolt extends BaseRichBolt {

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {

        }

        @Override
        public void execute(Tuple tuple) {

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static class TestSpout extends BaseRichSpout {

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        }

        @Override
        public void nextTuple() {

        }
    }
}
