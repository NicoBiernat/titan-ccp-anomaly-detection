package titan.ccp.anomalydetection.streamprocessing.detection;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.math3.util.Precision;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import titan.ccp.anomalydetection.api.client.StatisticsCache;
import titan.ccp.model.records.HourOfWeekActivePowerRecord;
import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.models.records.AggregatedActivePowerRecord;


/**
 * A statistical approach on anomaly detection. Using the data from the statistics microservice,
 * this implementation detects values that are further than three standard deviations away
 * from the mean of every hour of a week.
 */
public class StatisticalAnomalyDetection implements AnomalyDetection {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticalAnomalyDetection.class);

    private final StatisticsCache statisticsCache;

    private final ZoneId zoneId;

    private final int numStandardDeviations;

    private final double tolerance;

    private final double aggregatedTolerance;

    /**
     * Create a new {@link StatisticalAnomalyDetection}.
     * @param timeZone
     *  The timezone that should be used for converting timestamps into local date-time
     * @param numStandardDeviations
     *  The number of standard deviations that a record has to be away from the mean to be classified as an outlier
     */
    public StatisticalAnomalyDetection(final String timeZone, final int numStandardDeviations,
                                       final double tolerance, final double aggregatedTolerance) {
        this.statisticsCache = StatisticsCache.getInstance();
        this.zoneId = ZoneId.of(timeZone);
        this.numStandardDeviations = numStandardDeviations;
        this.tolerance = tolerance;
        this.aggregatedTolerance = aggregatedTolerance;
    }

    @Override
    public boolean activePowerRecordAnomalyDetection(final ActivePowerRecord record) {
        return detectAnomaly(record.getIdentifier(), record.getTimestamp(),
                record.getValueInW(), tolerance);
    }

    @Override
    public boolean aggregatedActivePowerRecordAnomalyDetection(final AggregatedActivePowerRecord record) {
        return detectAnomaly(record.getIdentifier(), record.getTimestamp(),
                record.getSumInW(), aggregatedTolerance);
    }

    private boolean detectAnomaly(final String identifier, final long timestamp,
                                  final double value, final double tolerance) {
        final Instant instant = Instant.ofEpochMilli(timestamp);
        final LocalDateTime dateTime = LocalDateTime.ofInstant(instant, this.zoneId);
        final int dayOfWeek = dateTime.getDayOfWeek().getValue();
        final int hour = dateTime.getHour();

        List<HourOfWeekActivePowerRecord> stats = statisticsCache.getHourOfWeek().get(identifier);
        if (stats == null) {
            return false;
        }
        stats = stats.stream().filter(elem -> elem.getDayOfWeek() == dayOfWeek && elem.getHourOfDay() == hour)
                     .collect(Collectors.toList());
        if (stats.isEmpty()) {
            return false; // not an anomaly if there is no prediction
        }
        if (stats.size() > 1) { // NOPMD
            LOGGER.warn("The statistics contain more than one record for a single hour-of-week!");
            return false;
        }
        final HourOfWeekActivePowerRecord prediction = stats.get(0);
        final boolean anomaly = isOutlier(value, prediction.getMean(), prediction.getPopulationVariance(), tolerance);
        if (LOGGER.isInfoEnabled() && anomaly) {
            LOGGER.info("  Day: {}", DayOfWeek.of(dayOfWeek).toString());
            LOGGER.info("  Hour: {}", hour);
            LOGGER.info("}");
        }
        return anomaly;
    }

    private boolean isOutlier(final double value, final double mean, final double variance, final double tolerance) {
        final double standardDeviation = Math.sqrt(variance);
        // round value and mean to neglect tiny differences smaller than one Watt
        final double valueRounded = Precision.round(value,0);
        final double meanRounded = Precision.round(mean, 0);
        final boolean outlier =
                Math.abs(meanRounded - valueRounded) > numStandardDeviations * standardDeviation + tolerance;
        if (LOGGER.isInfoEnabled() && outlier) {
            LOGGER.info("{");
            LOGGER.info("  Value: {}", value);
            LOGGER.info("  Mean: {}", mean);
            LOGGER.info("  Variance: {}", variance);
            LOGGER.info("  Standard Deviation: {}", standardDeviation);
            LOGGER.info("  {}x Standard Deviation: {}", numStandardDeviations,
                    numStandardDeviations * standardDeviation);
            LOGGER.info("  Difference Value-Mean: {}", Math.abs(value - mean));
        }

        return outlier;
    }
}
