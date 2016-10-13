---
title: Storm Scheduler Test Framework
layout: documentation
documentation: true
---
# Introduction

The purpose of this project is to have a framework that allows users to run simulations on scheduling strategies and evaluate the effectiveness of the schedulings via a set of metrics.  The framework can also allow users to run fuzz testing to validate that the scheduling strategies are need implementated correctly.

## Using the Framework

The framework is organized similar to Apache storm.  Performance tests or fuzz tests can be implemented and run as junit tests.

### Generating Random Topologies

The framework provides an implementation that will generate random topologies based on a set of paramenters.  The classpath of the implementation is:

    org.apache.storm.scheduler.performance.random.GenRandomTopology
    
### Generating Random Clusters

The framework provides an implementation that will generate random clusters, supervisors, and racks based on a set of paramenters.  The classpath of the implementation is:

    org.apache.storm.scheduler.performance.random.GenRandomCluster
    

### Framework Core Overview

The core of the framework consists of all the java classes in 
    
    org.apache.storm.scheduler.performance
    
They implement the fundamental functions of the test framework such as writing intermediate results to file (so we don't have to buffer a bunch of results in memory) and calculating stats based on results of the simulations.  

To prevent the need to have buffer a lot of data in memory from each simulation in which on heap memory could be a limit on how many iterations you run a simulation, intermediate results, in JSON format, are written to file in an online fashion.

### Running Tests

A number of test runners are implemented to faciliate running experiments using lower level API especially if you want to run experiment using multiple threads to accelerate simulations.  They are located in:
    
    org.apache.storm.scheduler.performance.runners
    
The DataSetPerformanceRunner is used to simulate schedulings based on a set of StormTopology objects and topology configs.

The RandomStrategyPerformanceRunner is used to simulate schedulings of randomly generated topologies and clusters

### Generating Stats using IPython Notebook/Jupyter Notebook

An IPython notebook file is located in 

    <project-root>/notebook/storm_scheduling_stats.ipynb
    
This file contains logic to generate stats based on results for a simulation run.  The implementation will attempt to read the results from a file name "results.json" in the same directory to get the results so remember to put the results in the same folder as the ipython notebook file.

### Examples

Examples of how to use the framework are the following (all under <project-root>/test/jvm/org/apache/storm/scheduler/performance/):
    
1. TestSchedulingStrategyPerformanceDataset.java
    * Provides an example of how you can use this framework to simulate scheduling topologies from a dataset in a multithreaded manner.
    * The dataset should be a set of **Thrift** serialized StormTopology objects in one folder and a set of **Java** serialized JSON string for the the corresponding topology confs.  An example is included the <project-root>/dataset folder
2. TestSchedulingStrategyPerformanceRandomTopologies.java
    * Provides an example of how you can use this framework to simulate schedulings of randomly generated topologies and clusters in a multithreaded manner
3. TestSchedulingStrategyPerformanceSampleTopologies
    * Provides an example on how to use the core API to simulate schedulings of custom defined topologies
    
    
Please note: Progress of simulations can be monitored in the surefire output file in <PROJECT_ROOT>/target/surefire-reports

Example on how to run one of the tests:

    mvn test -Dtest=TestConstraintSolver -DfailIfNoTests=false
    
