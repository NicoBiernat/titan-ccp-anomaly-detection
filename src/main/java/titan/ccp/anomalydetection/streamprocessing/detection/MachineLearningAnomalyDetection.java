package titan.ccp.anomalydetection.streamprocessing.detection;

import org.apache.kafka.streams.kstream.Predicate;
import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.models.records.AggregatedActivePowerRecord;

/**
 * Dummy anomaly detection implementation with a machine learning model.
 * This implementation does not work but shows how the machine learning forecast
 * might be integrated into this anomaly detection microservice.
 * This class is abstract so that nobody thinks of initializing it.
 */
@SuppressWarnings("unused")
public abstract class MachineLearningAnomalyDetection implements IAnomalyDetection {

    private static final IDummyMachineLearningModel ML_MODEL = null;

    @Override
    public Predicate<String, ActivePowerRecord> activePowerRecordAnomalyDetection() {
        return (key, record) -> detectAnomaly(record.getIdentifier(), record.getTimestamp(), record.getValueInW());
    }

    @Override
    public Predicate<String, AggregatedActivePowerRecord> aggregatedActivePowerRecordAnomalyDetection() {
        return (key, record) -> detectAnomaly(record.getIdentifier(), record.getTimestamp(), record.getSumInW());
    }

    private boolean detectAnomaly(final String identifier, final long timestamp, final double value) {
        final double prediction = ML_MODEL.forecast(identifier, timestamp);
        final double deviation  = ML_MODEL.deviation(identifier, timestamp);
        final boolean outlier =  Math.abs(value - prediction) < deviation;
        if (!outlier) {
            ML_MODEL.train(identifier, timestamp, value);
        }
        return outlier;
    }

    private interface IDummyMachineLearningModel {
        /**
         * Let the model forecast the next value of the sensor "identifier" at the time "timestamp".
         */
        double forecast(final String identifier, final long timestamp);

        /**
         * Return the precision of the forecast.
         */
        double deviation(final String identifier, final long timestamp);

        /**
         * Train the machine learning model with all records that are not an anomaly.
         */
        void train(final String identifier, final long timestamp, final double value);
    }
}
