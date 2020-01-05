package titan.ccp.anomalydetection.streamprocessing.detection;

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
    public boolean activePowerRecordAnomalyDetection(final ActivePowerRecord record) {
        return detectAnomaly(record.getIdentifier(), record.getTimestamp(), record.getValueInW());
    }

    @Override
    public boolean aggregatedActivePowerRecordAnomalyDetection(final AggregatedActivePowerRecord record) {
        return detectAnomaly(record.getIdentifier(), record.getTimestamp(), record.getSumInW());
    }

    private boolean detectAnomaly(final String identifier, final long timestamp, final double value) {
        final double prediction = ML_MODEL.forecast(identifier, timestamp);
        final double deviation  = ML_MODEL.deviation(identifier, timestamp);
        return Math.abs(value - prediction) < deviation;
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
    }
}
