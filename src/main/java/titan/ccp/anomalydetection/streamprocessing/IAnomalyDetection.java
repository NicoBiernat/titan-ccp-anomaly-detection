package titan.ccp.anomalydetection.streamprocessing;

import org.apache.kafka.streams.kstream.Predicate;
import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.models.records.AggregatedActivePowerRecord;

public interface IAnomalyDetection {
    Predicate<String, ActivePowerRecord> activePowerRecordAnomalyDetection();
    Predicate<String, AggregatedActivePowerRecord> aggregatedActivePowerRecordAnomalyDetection();
}
