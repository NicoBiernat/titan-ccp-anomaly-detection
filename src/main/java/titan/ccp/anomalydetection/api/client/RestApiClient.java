package titan.ccp.anomalydetection.api.client;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import titan.ccp.model.records.DayOfWeekActivePowerRecord;
import titan.ccp.model.records.HourOfDayActivePowerRecord;
import titan.ccp.model.records.HourOfWeekActivePowerRecord;
import titan.ccp.model.sensorregistry.SensorRegistry;
import titan.ccp.model.sensorregistry.SensorRegistryTraverser;

/**
 * REST-API Client for querying the Stats-Microservice and Config-Microservice APIs.
 */
public class RestApiClient {

    /* Part of the URL to query a sensors stats */
    private static final String SENSOR_STATS_URL_PART = "/sensor/";
    /* Base URL for the Stats-API */
    private final String statsBaseUrl;
    /* Base URL for the Config-API */
    private final String configBaseUrl;

    /* Http Client */
    private final HttpClient client;

    /* Json (de-)serializer. */
    private final Gson gson;

    /*
     * Types for the Json-Deserializer to use when creating Record-Lists from Json.
     */
    private final Type dayOfWeekActivePowerRecordListType // NOPMD
            = new TypeToken<ArrayList<DayOfWeekActivePowerRecord>>() {}.getType();
    private final Type hourOfDayActivePowerRecordListType // NOPMD
            = new TypeToken<ArrayList<HourOfDayActivePowerRecord>>() {}.getType();
    private final Type hourOfWeekActivePowerRecordListType // NOPMD
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
        this.client = HttpClient.newHttpClient();
        this.gson = new GsonBuilder().create();
    }

    /**
     * Makes a new asynchronous GET request to the Stats-APIs "day-of-week" route.
     * @param sensor The sensor-identifier to request for
     * @return
     * A {@link CompletableFuture} that is completed when the server has responded.
     */
    public CompletableFuture<List<DayOfWeekActivePowerRecord>> getDayOfWeek(final String sensor) {
        final HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(statsBaseUrl + SENSOR_STATS_URL_PART + sensor + "/day-of-week"))
                .build();
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(HttpResponse::body)
                .thenApply(json -> gson.fromJson(json, dayOfWeekActivePowerRecordListType));
    }

    /**
     * Makes a new asynchronous GET request to the Stats-APIs "hour-of-day" route.
     * @param sensor The sensor-identifier to request for
     * @return
     * A {@link CompletableFuture} that is completed when the server has responded.
     */
    public CompletableFuture<List<HourOfDayActivePowerRecord>> getHourOfDay(final String sensor) {
        final HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(statsBaseUrl + SENSOR_STATS_URL_PART + sensor + "/hour-of-day"))
                .build();
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(HttpResponse::body)
                .thenApply(json -> gson.fromJson(json, hourOfDayActivePowerRecordListType));
    }

    /**
     * Makes a new asynchronous GET request to the Stats-APIs "hour-of-week" route.
     * @param sensor The sensor-identifier to request for
     * @return
     * A {@link CompletableFuture} that is completed when the server has responded.
     */
    public CompletableFuture<List<HourOfWeekActivePowerRecord>> getHourOfWeek(final String sensor) {
        final HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(statsBaseUrl + SENSOR_STATS_URL_PART + sensor + "/hour-of-week"))
                .build();
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(HttpResponse::body)
                .thenApply(json -> gson.fromJson(json, hourOfWeekActivePowerRecordListType));
    }

    /**
     * Makes a new asynchronous GET request to the Config-APIs "sensor-registry" route.
     * @return
     * A {@link CompletableFuture} that is completed when the server has responded.
     */
    public CompletableFuture<List<String>> getSensorIdentifiers() {
        final HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(configBaseUrl + "/sensor-registry"))
                .build();
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(HttpResponse::body)
                .thenApply(SensorRegistry::fromJson)
                .thenApply(sensorRegistry -> {
                    final List<String> identifiers = new ArrayList<>();
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
