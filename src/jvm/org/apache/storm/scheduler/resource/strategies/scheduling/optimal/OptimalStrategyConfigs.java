package org.apache.storm.scheduler.resource.strategies.scheduling.optimal;

import org.apache.storm.validation.ConfigValidationAnnotations.CustomValidator;
import org.apache.storm.validation.ConfigValidationAnnotations.isInteger;
import org.apache.storm.validation.ConfigValidationAnnotations.isPositiveNumber;

import org.apache.storm.Config;

/**
 * Created by jerrypeng on 10/10/16.
 */
public class OptimalStrategyConfigs extends Config {
    /**
     * Declare scheduling constraints for a topology
     */
    public static final String TOPOLOGY_CONSTRAINTS = "topology.constraints";

    /**
     * Declare max traversal depth for find solutions that satisfy constraints
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_CONSTRAINTS_MAX_DEPTH_TRAVERSAL = "topology.constraints.max.depth.traversal";

}
