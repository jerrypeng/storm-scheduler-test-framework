package org.apache.storm.scheduler.performance.runners;

import org.apache.storm.Config;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.resource.strategies.scheduling.IStrategy;
import org.apache.storm.scheduler.performance.PerformanceUtils;
import org.apache.storm.scheduler.performance.random.GenRandomCluster;
import org.apache.storm.scheduler.performance.random.GenRandomTopology;

import java.util.Collection;
import java.util.Random;

/**
 * Created by jerrypeng on 9/16/16.
 */
public class RandomStrategyPerformanceRunner extends MultithreadedStrategyPerformanceRunner{

    @Override
    Object getRunArgs() {
        return null;
    }

    static class RunThread extends MultithreadedStrategyPerformanceRunner.RunThread {

        public RunThread(int iterations, Config defautConfs, String filename, Collection<Class<? extends IStrategy>> strategies, Object args) {
            super(iterations, defautConfs, filename, strategies, args);
        }

        @Override
        public void run() {
            Random random = new Random();
            for (int i = 0; i < this.iterations; i++) {
                Cluster cluster = new GenRandomCluster(this.defaultConfs).generateRandomCluster(6, 2, 3, 1000.0 * 8, 10000.0 * 8);
                this.defaultConfs.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, PerformanceUtils.getRandomIntInRange(0, 10000 * 8, random));
                this.perf.buildTestCluster(cluster);
                this.perf.addTestTopology(new GenRandomTopology(this.defaultConfs).generateRandomTopology(5, 1000.0, 10000.0));
                this.perf.run();
                this.perf.clearTestTopologies();
            }
            this.perf.endTest();
        }
    }

    public RandomStrategyPerformanceRunner(int numThreads, int numIterations, Config defaultConfs, String dataPath) {
        super(numThreads, numIterations, defaultConfs, dataPath, RunThread.class);
    }
}
