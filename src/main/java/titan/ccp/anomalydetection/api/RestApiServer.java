package titan.ccp.anomalydetection.api;

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
 * Contains a web server for accessing the history via a REST interface.
 */
public final class RestApiServer {
    // TODO make a builder that returns this server

    private static final Logger LOGGER = LoggerFactory.getLogger(RestApiServer.class);

    private final Gson gson = new GsonBuilder().create();

//    private final ActivePowerRepository<AggregatedActivePowerRecord> aggregatedRepository;
//    private final ActivePowerRepository<ActivePowerRecord> normalRepository;

    private final Service webService;

    private final boolean enableCors;
    private final boolean enableGzip;

    /**
     * Creates a new API server using the passed parameters.
     */
    public RestApiServer(final Session cassandraSession, final int port, final boolean enableCors,
                         final boolean enableGzip) {
//        this.aggregatedRepository = ActivePowerRepository.forAggregated(cassandraSession);
//        this.normalRepository = ActivePowerRepository.forNormal(cassandraSession);
        LOGGER.info("Instantiate API server.");
        System.out.println("Starting API Server");
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

        // TODO rename urls
        this.webService.get("/test", (request, response) -> {
            return "The AnomalyDetection API works!";
        }, this.gson::toJson);


//        this.webService.get("/power-consumption", (request, response) -> {
//            return this.normalRepository.getIdentifiers();
//        }, this.gson::toJson);
//
//        this.webService.get("/power-consumption/:identifier", (request, response) -> {
//            final String identifier = request.params("identifier"); // NOCS NOPMD
//            final long after = NumberUtils.toLong(request.queryParams("after"), 0); // NOCS NOPMD
//            return this.normalRepository.get(identifier, after);
//        }, this.gson::toJson);
//
//        this.webService.get("/power-consumption/:identifier/latest", (request, response) -> {
//            final String identifier = request.params("identifier");
//            final int count = NumberUtils.toInt(request.queryParams("count"), 1); // NOCS
//            return this.normalRepository.getLatest(identifier, count);
//        }, this.gson::toJson);
//
//        this.webService.get("/power-consumption/:identifier/distribution", (request, response) -> {
//            final String identifier = request.params("identifier");
//            final long after = NumberUtils.toLong(request.queryParams("after"), 0);
//            final int buckets = NumberUtils.toInt(request.queryParams("buckets"), 4); // NOCS
//            return this.normalRepository.getDistribution(identifier, after, buckets);
//        }, this.gson::toJson);
//
//        this.webService.get("/power-consumption/:identifier/trend", (request, response) -> {
//            final String identifier = request.params("identifier");
//            final long after = NumberUtils.toLong(request.queryParams("after"), 0);
//            final int pointsToSmooth = NumberUtils.toInt(request.queryParams("pointsToSmooth"), 10); // NOCS
//            return this.normalRepository.getTrend(identifier, after, pointsToSmooth);
//        }, this.gson::toJson);
//
//        this.webService.get("/power-consumption/:identifier/count", (request, response) -> {
//            final String identifier = request.params("identifier");
//            final long after = NumberUtils.toLong(request.queryParams("after"), 0);
//            return this.normalRepository.getCount(identifier, after);
//        }, this.gson::toJson);
//
//        // TODO Temporary for evaluation, this is not working for huge data sets
//        this.webService.get("/power-consumption-count", (request, response) -> {
//            return this.normalRepository.getTotalCount();
//        }, this.gson::toJson);
//
//        this.webService.get("/aggregated-power-consumption", (request, response) -> {
//            return this.aggregatedRepository.getIdentifiers();
//        }, this.gson::toJson);
//
//        this.webService.get("/aggregated-power-consumption/:identifier", (request, response) -> {
//            final String identifier = request.params("identifier");
//            final long after = NumberUtils.toLong(request.queryParams("after"), 0);
//            return this.aggregatedRepository.get(identifier, after);
//        }, this.gson::toJson);
//
//        this.webService.get("/aggregated-power-consumption/:identifier/latest", (request, response) -> {
//            final String identifier = request.params("identifier");
//            final int count = NumberUtils.toInt(request.queryParams("count"), 1);
//            return this.aggregatedRepository.getLatest(identifier, count);
//        }, this.gson::toJson);
//
//        this.webService.get("/aggregated-power-consumption/:identifier/distribution",
//                (request, response) -> {
//                    final String identifier = request.params("identifier");
//                    final long after = NumberUtils.toLong(request.queryParams("after"), 0);
//                    final int buckets = NumberUtils.toInt(request.queryParams("buckets"), 4);
//                    return this.aggregatedRepository.getDistribution(identifier, after, buckets);
//                }, this.gson::toJson);
//
//        this.webService.get("/aggregated-power-consumption/:identifier/trend", (request, response) -> {
//            final String identifier = request.params("identifier");
//            final long after = NumberUtils.toLong(request.queryParams("after"), 0);
//            final int pointsToSmooth = NumberUtils.toInt(request.queryParams("pointsToSmooth"), 10);
//            return this.aggregatedRepository.getTrend(identifier, after, pointsToSmooth);
//        }, this.gson::toJson);
//
//        this.webService.get("/aggregated-power-consumption/:identifier/count", (request, response) -> {
//            final String identifier = request.params("identifier");
//            final long after = NumberUtils.toLong(request.queryParams("after"), 0);
//            return this.aggregatedRepository.getCount(identifier, after);
//        }, this.gson::toJson);

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
