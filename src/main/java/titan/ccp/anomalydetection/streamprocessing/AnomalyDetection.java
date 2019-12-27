package titan.ccp.anomalydetection.streamprocessing;

import titan.ccp.anomalydetection.api.StatisticsCache;
import titan.ccp.model.records.DayOfWeekActivePowerRecord;
import titan.ccp.model.records.HourOfDayActivePowerRecord;
import titan.ccp.model.records.HourOfWeekActivePowerRecord;
import titan.ccp.models.records.ActivePowerRecord;

import org.apache.kafka.streams.kstream.Predicate;
import titan.ccp.models.records.AggregatedActivePowerRecord;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

public class AnomalyDetection implements IAnomalyDetection {

    private StatisticsCache statisticsCache;

    // todo variable epsilon
    private double epsilon = 30.0;

    public AnomalyDetection() {
        this.statisticsCache = StatisticsCache.getInstance();
    }

    @Override
    public Predicate<String, ActivePowerRecord> activePowerRecordAnomalyDetection() {
        return (key, record) -> {
            final Instant instant = Instant.ofEpochMilli(record.getTimestamp());
            final LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.of("Europe/Paris"));
            int dayOfWeek = dateTime.getDayOfWeek().getValue();
            int hour = dateTime.getHour();
            List<DayOfWeekActivePowerRecord> doW = statisticsCache.getDayOfWeek().get(key)
                    .stream().filter(elem -> elem.getDayOfWeek()==dayOfWeek)
                    .collect(Collectors.toList());
            List<HourOfDayActivePowerRecord> hoD = statisticsCache.getHourOfDay().get(key)
                    .stream().filter(elem -> elem.getHourOfDay()==hour)
                    .collect(Collectors.toList());
            List<HourOfWeekActivePowerRecord> hoW = statisticsCache.getHourOfWeek().get(key).
                    stream().filter(elem -> elem.getDayOfWeek()==dayOfWeek && elem.getHourOfDay()==hour)
                    .collect(Collectors.toList());

            if (doW.size() > 1 || hoD.size() > 1 || hoW.size() > 1) System.out.println("Something went horribly wrong!");

            boolean doWPrediction = false;
            boolean hoDPrediction = false;
            boolean hoWPrediction = false;

            if (!doW.isEmpty()) doWPrediction = Math.abs(doW.get(0).getMean() - record.getValueInW()) > 2 * Math.sqrt(doW.get(0).getPopulationVariance());
            if (!hoD.isEmpty()) hoDPrediction = Math.abs(hoD.get(0).getMean() - record.getValueInW()) > 2 * Math.sqrt(hoD.get(0).getPopulationVariance());
            if (!hoW.isEmpty()) hoWPrediction = Math.abs(hoW.get(0).getMean() - record.getValueInW()) > 2 * Math.sqrt(hoW.get(0).getPopulationVariance());
//          if (majority(doWPrediction, hoDPrediction, hoWPrediction)) {
            if (!doW.isEmpty() && !hoD.isEmpty() && !hoW.isEmpty()) {
                System.out.println("Sensor: " + key);
                System.out.println("DayOfWeek Mean: " + doW.get(0).getMean());
                System.out.println("HourOfDay Mean: " + hoD.get(0).getMean());
                System.out.println("HourOfWeek Mean: " + hoW.get(0).getMean());
                System.out.println("DayOfWeek Variance: " + doW.get(0).getPopulationVariance());
                System.out.println("HourOfDay Variance: " + hoD.get(0).getPopulationVariance());
                System.out.println("HourOfWeek Variance: " + hoW.get(0).getPopulationVariance());
            }
//          }

            return majority(doWPrediction, hoDPrediction, hoWPrediction);
        };
    }

    @Override
    public Predicate<String, AggregatedActivePowerRecord> aggregatedActivePowerRecordAnomalyDetection() {
        return (key, record) -> false; //todo
    }

    private boolean majority(boolean x, boolean y, boolean z) {
        return (x && y) || (y && z) || (x && z);
    }
}
