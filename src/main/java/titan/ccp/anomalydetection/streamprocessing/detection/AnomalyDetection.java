package titan.ccp.anomalydetection.streamprocessing.detection;

import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.models.records.AggregatedActivePowerRecord;

/**
 * An interface for Anomaly-Detection implementations.
 */
public interface AnomalyDetection {

    /**
     * Determines whether an {@link ActivePowerRecord} is an anomaly or not.
     * @return
     * true, if the record was classified as an anomaly
     */
    boolean activePowerRecordAnomalyDetection(final ActivePowerRecord record);

    /**
     * Determines whether {@link AggregatedActivePowerRecord} is an anomaly or not.
     * @return
     * true, if the record was classified as an anomaly
     */
    boolean aggregatedActivePowerRecordAnomalyDetection(final AggregatedActivePowerRecord record);
}
