package org.apache.storm.scheduler.resource;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.SupervisorDetails;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jerrypeng on 9/20/16.
 */
public class ResourceExtraUtils extends ResourceUtils {
    public static double sum(Collection<Double> list) {
        double sum = 0.0;
        for (Double elem : list) {
            sum += elem;
        }
        return sum;
    }

    public static double avg(Collection<Double> list) {
        return sum(list) / list.size();
    }

    public static Map<String, String> getSupIdToRack(Cluster cluster) {
        Map<String, String> supIdsToRack = new HashMap<String, String>();
        for (Map.Entry<String, List<String>> entry : cluster.getNetworkTopography().entrySet()) {
            String rackId = entry.getKey();
            List<String> hosts = entry.getValue();
            for (String host : hosts) {
                for (SupervisorDetails supervisorDetails : cluster.getSupervisorsByHost(host)) {
                    supIdsToRack.put(supervisorDetails.getId(), rackId);
                }
            }
        }
        return supIdsToRack;
    }
}
