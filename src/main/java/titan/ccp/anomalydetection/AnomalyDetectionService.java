package titan.ccp.anomalydetection;

import java.util.concurrent.CompletableFuture;

import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.streams.KafkaStreams;
import titan.ccp.anomalydetection.api.RestApiServer;
import titan.ccp.anomalydetection.api.StatisticsCache;
import titan.ccp.common.cassandra.SessionBuilder;
import titan.ccp.common.cassandra.SessionBuilder.ClusterSession;
import titan.ccp.common.configuration.Configurations;
import titan.ccp.anomalydetection.streamprocessing.KafkaStreamsBuilder;

/**
 * Anomaly-Detection Microservice
 */
public final class AnomalyDetectionService {

    private final Configuration config = Configurations.create();

    private final CompletableFuture<Void> stopEvent = new CompletableFuture<>();

    /**
     * Start the service.
     *
     * @return {@link CompletableFuture} which is completed when the service is successfully started.
     */
    public CompletableFuture<Void> run() {
        System.out.println("Starting Anomaly-Detection Service...");

        final CompletableFuture<ClusterSession> clusterSessionStarter =
                CompletableFuture.supplyAsync(this::startCassandraSession);

        final CompletableFuture<Void> kafkaStartedEvent =
                clusterSessionStarter.thenAcceptAsync(this::createKafkaStreamsApplication);

        final CompletableFuture<Void> restApiServerStartedEvent =
                clusterSessionStarter.thenAcceptAsync(this::startWebserver);

        final CompletableFuture<Void> restApiClientStartedEvent =
                CompletableFuture.runAsync(this::startWebclient);

        return CompletableFuture.allOf(kafkaStartedEvent, restApiServerStartedEvent, restApiClientStartedEvent);
    }

    /**
     * Connect to the database.
     *
     * @return the {@link ClusterSession} for the cassandra cluster.
     */
    private ClusterSession startCassandraSession() {
        final ClusterSession clusterSession = new SessionBuilder()
                .contactPoint(this.config.getString(ConfigurationKeys.CASSANDRA_HOST))
                .port(this.config.getInt(ConfigurationKeys.CASSANDRA_PORT))
                .keyspace(this.config.getString(ConfigurationKeys.CASSANDRA_KEYSPACE))
                .timeoutInMillis(this.config.getInt(ConfigurationKeys.CASSANDRA_INIT_TIMEOUT_MS))
                .build();
        this.stopEvent.thenRun(clusterSession.getSession()::close);
        return clusterSession;
    }

    /**
     * Build and start the underlying Kafka Streams application of the service.
     *
     * (at)param clusterSession the database session which the application should use.
     */
    private void createKafkaStreamsApplication(final ClusterSession clusterSession) {
        final KafkaStreams kafkaStreams = new KafkaStreamsBuilder()
                .cassandraSession(clusterSession.getSession())
                .bootstrapServers(this.config.getString(ConfigurationKeys.KAFKA_BOOTSTRAP_SERVERS))
                .inputTopic(this.config.getString(ConfigurationKeys.KAFKA_INPUT_TOPIC))
                .outputTopic(this.config.getString(ConfigurationKeys.KAFKA_OUTPUT_TOPIC))
                .numThreads(this.config.getInt(ConfigurationKeys.NUM_THREADS))
                .commitIntervalMs(this.config.getInt(ConfigurationKeys.COMMIT_INTERVAL_MS))
                .cacheMaxBytesBuffering(this.config.getInt(ConfigurationKeys.CACHE_MAX_BYTES_BUFFERING))
                .build();
        this.stopEvent.thenRun(kafkaStreams::close);
        kafkaStreams.start();
    }

    /**
     * Start the webserver of the service.
     *
     * (at)param clusterSession the database session which the server should use.
     */
    private void startWebserver(final ClusterSession clusterSession) {
        if (this.config.getBoolean(ConfigurationKeys.WEBSERVER_ENABLE)) {
            final RestApiServer restApiServer = new RestApiServer(
                    clusterSession.getSession(),
                    this.config.getInt(ConfigurationKeys.WEBSERVER_PORT),
                    this.config.getBoolean(ConfigurationKeys.WEBSERVER_CORS),
                    this.config.getBoolean(ConfigurationKeys.WEBSERVER_GZIP));
            this.stopEvent.thenRun(restApiServer::stop);
            restApiServer.start();
        }
    }

    private void startWebclient() {
        StatisticsCache statisticsCache = StatisticsCache.getInstance();
        this.stopEvent.thenRun(statisticsCache::stopUpdater);
        statisticsCache.startUpdater(
                this.config.getString(ConfigurationKeys.STATS_HOST),
                this.config.getString(ConfigurationKeys.CONFIG_HOST)
        );
    }

    /**
     * Stop the service.
     */
    public void stop() {
        this.stopEvent.complete(null);
    }

    public static void main(final String[] args) {
        new AnomalyDetectionService().run().join();
        System.out.println("Anomaly-Detection Service terminated.");
    }
}