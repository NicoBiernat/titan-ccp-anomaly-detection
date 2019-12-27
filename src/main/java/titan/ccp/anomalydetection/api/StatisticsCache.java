package titan.ccp.anomalydetection.api;

import titan.ccp.model.records.DayOfWeekActivePowerRecord;
import titan.ccp.model.records.HourOfDayActivePowerRecord;
import titan.ccp.model.records.HourOfWeekActivePowerRecord;

import java.util.*;
import java.util.concurrent.*;

/**
 * A cache containing the data from the Statistics REST-API with a builtin automatic updater.
 */
public final class StatisticsCache {

    private static final int POLLING_RATE_MS = 1000; // TODO as config parameter

    private static StatisticsCache INSTANCE;

    /** List of sensor identifiers (from Configuration REST-API) that act as the key for the cache. */
    private List<String> sensorIdentifiers;

    /*
     *  In memory cache using HashMaps.
     */
    private Map<String, List<DayOfWeekActivePowerRecord>> dayOfWeek = new HashMap<>();
    private Map<String, List<HourOfDayActivePowerRecord>> hourOfDay = new HashMap<>();
    private Map<String, List<HourOfWeekActivePowerRecord>> hourOfWeek = new HashMap<>();

    private RestApiClient client;
    /** The updater thread.*/
//    private Updater updater;

    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    /*
     * Singleton
     */
    private StatisticsCache() {}

    public static StatisticsCache getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new StatisticsCache();
        }
        return INSTANCE;
    }

    /**
     * Starts the automatic cache updater thread.
     * @param statsBaseUrl
     *      The base URL of the Stats Microservices REST-API
     * @param configBaseUrl
     *      The base URL of the Config Microservices REST-API
     */
    public void startUpdater(String statsBaseUrl, String configBaseUrl) {
        this.client = new RestApiClient(statsBaseUrl, configBaseUrl);

        Runnable updater = () -> {
            try {
                client.getSensorIdentifiers()
                        .thenApply(identifiers -> sensorIdentifiers = identifiers)
                        .get(); // wait for identifiers to arrive
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            // request Stats-API asynchronously
            List<CompletableFuture<Void>> done = new ArrayList<>();
            sensorIdentifiers.forEach(sensor -> {
                done.add(CompletableFuture.allOf(
                        client.getDayOfWeek(sensor).thenApply(list -> dayOfWeek.put(sensor, list)),
                        client.getHourOfDay(sensor).thenApply(list -> hourOfDay.put(sensor, list)),
                        client.getHourOfWeek(sensor).thenApply(list -> hourOfWeek.put(sensor, list))
                ));
            });
            // wait for updates to complete
            for (CompletableFuture<Void> f : done) {
                try {
                    f.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("Cache updated");
        };

        this.scheduler.scheduleAtFixedRate(updater,0,POLLING_RATE_MS, TimeUnit.MILLISECONDS);
//        this.updater = new Updater(statsBaseUrl, configBaseUrl, POLLING_RATE_MS);
//        System.out.println("Starting updater...");
//        this.updater.start();
    }

    /**
     * Stops the automatic cache updater thread.
     */
    public void stopUpdater() {
//        updater.halt();
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

//    /**
//     * The cache updater thread.
//     */
//    private final class Updater extends Thread {
//
//        private RestApiClient client;
//        private int pollingRate;
//        private boolean stopSignal = true;
//
//        private Updater(String statsBaseUrl, String configBaseUrl, int pollingRateInMs) {
//            this.client = new RestApiClient(statsBaseUrl, configBaseUrl);
//            this.pollingRate = pollingRateInMs;
//        }
//
//        private void update() throws ExecutionException, InterruptedException {
//            client.getSensorIdentifiers()
//                    .thenApply(identifiers -> sensorIdentifiers = identifiers)
//                    .get(); // wait for identifiers to arrive
//            // request Stats-API asynchronously
//            List<CompletableFuture<Void>> done = new ArrayList<>();
//            sensorIdentifiers.forEach(sensor -> {
//                done.add(CompletableFuture.allOf(
//                    client.getDayOfWeek(sensor).thenApply(list -> dayOfWeek.put(sensor, list)),
//                    client.getHourOfDay(sensor).thenApply(list -> hourOfDay.put(sensor, list)),
//                    client.getHourOfWeek(sensor).thenApply(list -> hourOfWeek.put(sensor, list))
//                ));
//            });
//            // wait for updates to complete
//            for (CompletableFuture<Void> f : done) {
//                f.get();
//            }
//            System.out.println("Cache updated");
//        }
//
//        @Override
//        public void run() { // todo: use scheduled executor service
//            while (true) {
//                try {
//                    update();
//                } catch (ExecutionException | InterruptedException e) {
//                    e.printStackTrace();
//                }
//                if (this.stopSignal) break;
//                try {
//                    sleep(pollingRate);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//
//        @Override
//        public synchronized void start() {
//            this.stopSignal = false;
//            super.start();
//        }
//
//        /**
//         * Stops the updater gracefully.
//         */
//        public synchronized void halt() {
//            this.stopSignal = true;
//        }
//    }

}
