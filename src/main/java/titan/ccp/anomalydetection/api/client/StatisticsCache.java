package titan.ccp.anomalydetection.api.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import titan.ccp.model.records.DayOfWeekActivePowerRecord;
import titan.ccp.model.records.HourOfDayActivePowerRecord;
import titan.ccp.model.records.HourOfWeekActivePowerRecord;

/**
 * A cache containing the data from the Statistics REST-API with a builtin automatic updater.
 */
public final class StatisticsCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsCache.class);

    private static StatisticsCache instance = new StatisticsCache();

    /* List of sensor identifiers (from Configuration REST-API) that act as the key for the cache. */
    private List<String> sensorIdentifiers;

    /*
     *  In-memory cache using HashMaps.
     */
    private final Map<String, List<DayOfWeekActivePowerRecord>> dayOfWeek = new HashMap<>();
    private final Map<String, List<HourOfDayActivePowerRecord>> hourOfDay = new HashMap<>();
    private final Map<String, List<HourOfWeekActivePowerRecord>> hourOfWeek = new HashMap<>();

    /*
     * RestApiClient for polling data
     */
    private RestApiClient client;

    /*
     * Updates the cache periodically
     */
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    /*
     * Singleton
     */
    private StatisticsCache() {}

    /**
     * Returns the Singleton-Instance of the StatisticsCache.
     * @return
     * The instance
     */
    public static StatisticsCache getInstance() {
        return instance;
    }

    /**
     * Starts the automatic cache updater.
     * @param statsBaseUrl
     *      The base URL of the Stats Microservices REST-API
     * @param configBaseUrl
     *      The base URL of the Config Microservices REST-API
     */
    public void startUpdater(final int pollingRateMs, final String statsBaseUrl, final String configBaseUrl) {
        this.client = new RestApiClient(statsBaseUrl, configBaseUrl);

        this.scheduler.scheduleAtFixedRate(() -> {
            try {
                client.getSensorIdentifiers()
                        .thenApply(identifiers -> sensorIdentifiers = identifiers)
                        .get(); // wait for identifiers to arrive
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.warn(e.getStackTrace().toString());
            }

            // request Stats-API asynchronously
            final List<CompletableFuture<Void>> done = new ArrayList<>();
            sensorIdentifiers.forEach(sensor -> {
                done.add(CompletableFuture.allOf(
                        client.getDayOfWeek(sensor).thenApply(list -> dayOfWeek.put(sensor, list)),
                        client.getHourOfDay(sensor).thenApply(list -> hourOfDay.put(sensor, list)),
                        client.getHourOfWeek(sensor).thenApply(list -> hourOfWeek.put(sensor, list))
                ));
            });
            // wait for updates to complete
            for (final CompletableFuture<Void> f : done) {
                try {
                    f.get();
                } catch (InterruptedException | ExecutionException e) {
                    LOGGER.warn(e.getStackTrace().toString());
                }
            }
        }, 0, pollingRateMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Stops the automatic cache updater thread.
     */
    public void stopUpdater() {
        scheduler.shutdownNow();
    }

    /*
     * Getters
     */
    public Map<String, List<DayOfWeekActivePowerRecord>> getDayOfWeek() {
        return dayOfWeek;
    }

    public Map<String, List<HourOfDayActivePowerRecord>> getHourOfDay() {
        return hourOfDay;
    }

    public Map<String, List<HourOfWeekActivePowerRecord>> getHourOfWeek() {
        return hourOfWeek;
    }

}
