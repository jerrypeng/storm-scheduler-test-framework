package org.apache.storm.scheduler.performance.runners;

import org.apache.storm.Config;
import org.apache.storm.scheduler.performance.PerformanceTestReport;
import org.apache.storm.scheduler.resource.strategies.scheduling.IStrategy;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jerrypeng on 9/15/16.
 */
public abstract class AbstractStrategyPerformanceRunner {
    Config defaultConfs;
    List<Class<? extends IStrategy>> strategies = new LinkedList<>();
    String dataPath;

    public AbstractStrategyPerformanceRunner(Config defaultConfs, String dataPath) {
        this.defaultConfs = defaultConfs;
        this.dataPath = dataPath;
        File dataPathFile = new File(this.dataPath);
        if (dataPathFile.exists() && dataPathFile.isDirectory()) {
            try {
                FileUtils.cleanDirectory(dataPathFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void addStrategy(Class<? extends IStrategy> strategy) {
        this.strategies.add(strategy);
    }

    public abstract void run();

    public void createTestReport(String testReportPath) {

        PerformanceTestReport report = new PerformanceTestReport(this.dataPath);

        report.generateReport(testReportPath);
    }
}
