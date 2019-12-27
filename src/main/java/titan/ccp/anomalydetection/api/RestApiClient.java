package titan.ccp.anomalydetection.api;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import okhttp3.OkHttpClient;
import okhttp3.Request;

import titan.ccp.model.records.DayOfWeekActivePowerRecord;
import titan.ccp.model.records.HourOfDayActivePowerRecord;
import titan.ccp.model.records.HourOfWeekActivePowerRecord;
import titan.ccp.model.sensorregistry.SensorRegistry;
import titan.ccp.model.sensorregistry.SensorRegistryTraverser;

/**
 * REST-API Client using the OkHttp-Library for querying the Stats-Microservice and Config-Microservice APIs.
 */
public final class RestApiClient {

    /** Base URL for the Stats-API */
    private final String statsBaseUrl;
    /** Base URL for the Config-API */
    private final String configBaseUrl;

    /** Http Client */
    private final OkHttpClient client;

    /** Json (de-)serializer. */
    private final Gson gson;

    /*
     * Types for the Json-Deserializer to use when creating Record-Lists from Json.
     */
    private static final Type dayOfWeekActivePowerRecordListType
            = new TypeToken<ArrayList<DayOfWeekActivePowerRecord>>() {}.getType();
    private static final Type hourOfDayActivePowerRecordListType
            = new TypeToken<ArrayList<HourOfDayActivePowerRecord>>() {}.getType();
    private static final Type hourOfWeekActivePowerRecordListType
            = new TypeToken<ArrayList<HourOfWeekActivePowerRecord>>() {}.getType();

    /**
     * Creates a new RestApiClient with the given base URLs.
     * @param statsBaseUrl
     * Base URL for the Stats-API
     * @param configBaseUrl
     * Base URL for the Config-API
     */
    public RestApiClient(final String statsBaseUrl, final String configBaseUrl) {
        this.statsBaseUrl = statsBaseUrl;
        this.configBaseUrl = configBaseUrl;
        this.client = new OkHttpClient();
        this.gson = new GsonBuilder().create();
    }

//    public void test() {
//        List<DayOfWeekActivePowerRecord> dOfW = null;
//        List<HourOfDayActivePowerRecord> hOfD = null;
//        List<HourOfWeekActivePowerRecord> hOfW = null;
//        try {
//            dOfW = getDayOfWeek("root").get();
//            hOfD = getHourOfDay("root").get();
//            hOfW = getHourOfWeek("root").get();
//        } catch (ExecutionException | InterruptedException e) {
//            e.printStackTrace();
//        }
//        System.out.println("Mean values for day-of-week:");
//        dOfW.forEach(r -> System.out.println(r.getDayOfWeek()+": "+r.getMean()));
//        System.out.println("Mean values for hour-of-day:");
//        hOfD.forEach(r -> System.out.println(r.getHourOfDay()+": "+r.getMean()));
//        System.out.println("Mean values for hour-of-week:");
//        hOfW.forEach(r -> System.out.println("("+r.getDayOfWeek()+","+r.getHourOfDay()+"): "+r.getMean()));
//
//        System.out.println("Sensor-Identifiers: ");
//        try {
//            getSensorIdentifiers().get().forEach(i -> System.out.println(i));
//        } catch (InterruptedException | ExecutionException e) {
//            e.printStackTrace();
//        }
//    }

    /**
     * Makes a new asynchronous GET request to the Stats-APIs "day-of-week" route.
     * @param sensor The sensor-identifier to request for
     * @return
     * A {@link CompletableFuture<List<DayOfWeekActivePowerRecord>>} that is completed when the server has responded.
     */
    public CompletableFuture<List<DayOfWeekActivePowerRecord>> getDayOfWeek(final String sensor) {
        final Request request = new Request.Builder()
                .get()
                .url(statsBaseUrl + "/sensor/" + sensor + "/day-of-week")
                .build();
        CompletableFuture<String> future = new CompletableFuture<>();
        client.newCall(request).enqueue(new FutureCallback(future));
        return future.thenApply(json -> gson.fromJson(json, dayOfWeekActivePowerRecordListType));
    }

    /**
     * Makes a new asynchronous GET request to the Stats-APIs "hour-of-day" route.
     * @param sensor The sensor-identifier to request for
     * @return
     * A {@link CompletableFuture<List<HourOfDayActivePowerRecord>>} that is completed when the server has responded.
     */
    public CompletableFuture<List<HourOfDayActivePowerRecord>> getHourOfDay(final String sensor) {
        final Request request = new Request.Builder()
                .get()
                .url(statsBaseUrl + "/sensor/" + sensor + "/hour-of-day")
                .build();
        CompletableFuture<String> future = new CompletableFuture<>();
        client.newCall(request).enqueue(new FutureCallback(future));
        return future.thenApply(json -> gson.fromJson(json, hourOfDayActivePowerRecordListType));
    }

    /**
     * Makes a new asynchronous GET request to the Stats-APIs "hour-of-week" route.
     * @param sensor The sensor-identifier to request for
     * @return
     * A {@link CompletableFuture<List<HourOfWeekActivePowerRecord>>} that is completed when the server has responded.
     */
    public CompletableFuture<List<HourOfWeekActivePowerRecord>> getHourOfWeek(final String sensor){
        final Request request = new Request.Builder()
                .get()
                .url(statsBaseUrl + "/sensor/" + sensor + "/hour-of-week")
                .build();
        CompletableFuture<String> future = new CompletableFuture<>();
        client.newCall(request).enqueue(new FutureCallback(future));
        return future.thenApply(json -> gson.fromJson(json, hourOfWeekActivePowerRecordListType));
    }

    /**
     * Makes a new asynchronous GET request to the Config-APIs "sensor-registry" route.
     * @return
     * A {@link CompletableFuture<List<String>>} that is completed when the server has responded.
     */
    public CompletableFuture<List<String>> getSensorIdentifiers() {
        final Request request = new Request.Builder()
                .get()
                .url(configBaseUrl + "/sensor-registry")
                .build();
        CompletableFuture<String> future = new CompletableFuture<>();
        client.newCall(request).enqueue(new FutureCallback(future));
        return future
                .thenApply(SensorRegistry::fromJson)
                .thenApply(sensorRegistry -> {
                    List<String> identifiers = new ArrayList<>();
                    new SensorRegistryTraverser().traverseAggregated(
                            sensorRegistry,
                            aggregatedSensor -> identifiers.add(aggregatedSensor.getIdentifier())
                    );
                    sensorRegistry.getMachineSensors().forEach(
                            machineSensor -> identifiers.add(machineSensor.getIdentifier())
                    );
                    return identifiers;
                });
    }
}
