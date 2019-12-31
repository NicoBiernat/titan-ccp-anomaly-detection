package titan.ccp.anomalydetection.streamprocessing.detection;

import org.apache.kafka.streams.kstream.Predicate;
import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.models.records.AggregatedActivePowerRecord;

/**
 * An interface for Anomaly-Detection implementations.
 */
public interface IAnomalyDetection {

    /**
     * Returns a {@link Predicate} that determines if an {@link ActivePowerRecord} is an anomaly or not.
     * @return
     * The predicate
     */
    Predicate<String, ActivePowerRecord> activePowerRecordAnomalyDetection();

    /**
     * Returns a {@link Predicate} that determines if an {@link AggregatedActivePowerRecord} is an anomaly or not.
     * @return
     * The predicate
     */
    Predicate<String, AggregatedActivePowerRecord> aggregatedActivePowerRecordAnomalyDetection();
}
