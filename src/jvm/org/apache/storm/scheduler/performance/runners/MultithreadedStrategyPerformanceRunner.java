package org.apache.storm.scheduler.performance.runners;

import org.apache.storm.Config;
import org.apache.storm.scheduler.performance.PerformanceTestSuite;
import org.apache.storm.scheduler.resource.strategies.scheduling.IStrategy;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jerrypeng on 9/12/16.
 */
public abstract class MultithreadedStrategyPerformanceRunner extends AbstractStrategyPerformanceRunner {

    final int iterationsPerThread;
    final int threads;
    final Class<? extends Runnable> runThreadClass;

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(MultithreadedStrategyPerformanceRunner.class);

    static abstract class RunThread implements Runnable {
        final PerformanceTestSuite perf;
        final int iterations;
        final Config defaultConfs = new Config();
        final Object args;

        public RunThread (int iterations, Config defautConfs, String filename, Collection<Class<? extends IStrategy>> strategies, Object args) {
            this.perf = new PerformanceTestSuite(defautConfs, filename);
            for (Class<? extends IStrategy> strategy : strategies) {
                this.perf.addSchedulingStrategyToTest(strategy);
            }
            this.defaultConfs.putAll(defautConfs);
            this.iterations = iterations;
            this.args = args;
        }
    }

    public MultithreadedStrategyPerformanceRunner(int numThreads, int numIterations, Config defaultConfs, String dataPath, Class<? extends RunThread> runThreadClass) {
        super(defaultConfs, dataPath);
        this.threads = Math.min(numThreads, numIterations);
        this.iterationsPerThread = numIterations/this.threads;
        LOG.info("threads: {} iterationsPerThread: {}", this.threads, this.iterationsPerThread);
        this.defaultConfs = defaultConfs;
        this.dataPath = dataPath;
        this.runThreadClass = runThreadClass;
    }

    abstract Object getRunArgs();

    public void run() {
        List<Thread> threads = new LinkedList<>();
        Object args = getRunArgs();
        try {
            for (int i = 0; i < this.threads; i++) {
                Thread thread = new Thread(this.runThreadClass.getConstructor(int.class, Config.class, String.class, Collection.class, Object.class).newInstance(this.iterationsPerThread, this.defaultConfs, this.dataPath + "/JsonLog.log." + i, this.strategies, args));
                threads.add(thread);
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }
        } catch (InterruptedException | NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void createTestReport() {
        createTestReport("notebook/rawResults.json");
    }
}
