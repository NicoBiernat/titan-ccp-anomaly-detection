package titan.ccp.anomalydetection.api.server;

import com.datastax.driver.core.Session;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Service;
import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.models.records.AggregatedActivePowerRecord;

/**
 * Contains a web server for accessing the power anomalies via a REST interface.
 * This API is modeled after the API from the History-Microservice
 * to allow easier adaptation by the frontend.
 * A trend does not make that much sense for anomalies, so that route was left out.
 */
public class RestApiServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RestApiServer.class);

    private final Gson gson = new GsonBuilder().create();

    private final ActivePowerRepository<AggregatedActivePowerRecord> aggregatedRepository;
    private final ActivePowerRepository<ActivePowerRecord> normalRepository;

    private final Service webService;

    private final boolean enableCors;
    private final boolean enableGzip;

    /**
     * Creates a new API server using the passed parameters.
     */
    public RestApiServer(final Session cassandraSession, final String tableNameSuffix,
                         final int port, final boolean enableCors, final boolean enableGzip) {
        this.aggregatedRepository = ActivePowerRepository.forAggregated(cassandraSession, tableNameSuffix);
        this.normalRepository = ActivePowerRepository.forNormal(cassandraSession, tableNameSuffix);
        LOGGER.info("Instantiate API server.");
        this.webService = Service.ignite().port(port);
        this.enableCors = enableCors;
        this.enableGzip = enableGzip;
    }

    /**
     * Start the web server by setting up the API routes.
     */
    public void start() { // NOPMD declaration of routes
        LOGGER.info("Instantiate API routes.");

        if (this.enableCors) {
            this.webService.options("/*", (request, response) -> {

                final String accessControlRequestHeaders =
                        request.headers("Access-Control-Request-Headers");
                if (accessControlRequestHeaders != null) {
                    response.header("Access-Control-Allow-Headers", accessControlRequestHeaders);
                }

                final String accessControlRequestMethod = request.headers("Access-Control-Request-Method");
                if (accessControlRequestMethod != null) {
                    response.header("Access-Control-Allow-Methods", accessControlRequestMethod);
                }

                return "OK";
            });

            this.webService.before((request, response) -> {
                response.header("Access-Control-Allow-Origin", "*");
            });
        }

        this.webService.get("/power-anomaly", (request, response) -> {
            return this.normalRepository.getIdentifiers();
        }, this.gson::toJson);

        this.webService.get("/power-anomaly/:identifier", (request, response) -> {
            final String identifier = request.params("identifier"); // NOCS NOPMD
            final long after = NumberUtils.toLong(request.queryParams("after"), 0); // NOCS NOPMD
            return this.normalRepository.get(identifier, after);
        }, this.gson::toJson);

        this.webService.get("/power-anomaly/:identifier/latest", (request, response) -> {
            final String identifier = request.params("identifier");
            final int count = NumberUtils.toInt(request.queryParams("count"), 1); // NOCS
            return this.normalRepository.getLatest(identifier, count);
        }, this.gson::toJson);

        this.webService.get("/power-anomaly/:identifier/distribution", (request, response) -> {
            final String identifier = request.params("identifier");
            final long after = NumberUtils.toLong(request.queryParams("after"), 0);
            final int buckets = NumberUtils.toInt(request.queryParams("buckets"), 4); // NOCS
            return this.normalRepository.getDistribution(identifier, after, buckets);
        }, this.gson::toJson);

        this.webService.get("/power-anomaly/:identifier/count", (request, response) -> {
            final String identifier = request.params("identifier");
            final long after = NumberUtils.toLong(request.queryParams("after"), 0);
            return this.normalRepository.getCount(identifier, after);
        }, this.gson::toJson);

        // TODO Temporary for evaluation, this is not working for huge data sets
        this.webService.get("/power-anomaly-count", (request, response) -> {
            return this.normalRepository.getTotalCount();
        }, this.gson::toJson);

        this.webService.get("/aggregated-power-anomaly", (request, response) -> {
            return this.aggregatedRepository.getIdentifiers();
        }, this.gson::toJson);

        this.webService.get("/aggregated-power-anomaly/:identifier", (request, response) -> {
            final String identifier = request.params("identifier");
            final long after = NumberUtils.toLong(request.queryParams("after"), 0);
            return this.aggregatedRepository.get(identifier, after);
        }, this.gson::toJson);

        this.webService.get("/aggregated-power-anomaly/:identifier/latest", (request, response) -> {
            final String identifier = request.params("identifier");
            final int count = NumberUtils.toInt(request.queryParams("count"), 1);
            return this.aggregatedRepository.getLatest(identifier, count);
        }, this.gson::toJson);

        this.webService.get("/aggregated-power-anomaly/:identifier/distribution", (request, response) -> {
            final String identifier = request.params("identifier");
            final long after = NumberUtils.toLong(request.queryParams("after"), 0);
            final int buckets = NumberUtils.toInt(request.queryParams("buckets"), 4);
            return this.aggregatedRepository.getDistribution(identifier, after, buckets);
        }, this.gson::toJson);

        this.webService.get("/aggregated-power-anomaly/:identifier/count", (request, response) -> {
            final String identifier = request.params("identifier");
            final long after = NumberUtils.toLong(request.queryParams("after"), 0);
            return this.aggregatedRepository.getCount(identifier, after);
        }, this.gson::toJson);

        // TODO Temporary for evaluation, this is not working for huge data sets
        this.webService.get("/aggregated-power-anomaly-count", (request, response) -> {
            return this.aggregatedRepository.getTotalCount();
        }, this.gson::toJson);

        this.webService.after((request, response) -> {
            response.type("application/json");
            if (this.enableGzip) {
                response.header("Content-Encoding", "gzip");
            }
        });
    }

    /**
     * Stop the webserver.
     */
    public void stop() {
        this.webService.stop();
    }

}
