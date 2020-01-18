package titan.ccp.anomalydetection.streamprocessing;

import com.datastax.driver.core.Session;

import java.util.Objects;
import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import titan.ccp.anomalydetection.streamprocessing.detection.AnomalyDetection;
import titan.ccp.common.kafka.streams.PropertiesBuilder;

/**
 * Builder for the Kafka Streams configuration.
 */
public class KafkaStreamsBuilder {

    private static final String APPLICATION_NAME = "titan-ccp-anomalydetection";
    private static final String APPLICATION_VERSION = "0.0.1";

    private AnomalyDetection anomalyDetection; // NOPMD
    private Session cassandraSession; // NOPMD
    private String tableNameSuffix; // NOPMD
    private String bootstrapServers; // NOPMD
    private String inputTopic; // NOPMD
    private String outputTopic; // NOPMD
    private int numThreads = -1; // NOPMD
    private int commitIntervalMs = -1; // NOPMD
    private int cacheMaxBytesBuff = -1; // NOPMD

    public KafkaStreamsBuilder anomalyDetection(final AnomalyDetection anomalyDetection) {
        this.anomalyDetection = anomalyDetection;
        return this;
    }

    public KafkaStreamsBuilder cassandraSession(final Session cassandraSession) {
        this.cassandraSession = cassandraSession;
        return this;
    }

    public KafkaStreamsBuilder tableNameSuffix(final String tableNameSuffix) {
        this.tableNameSuffix = tableNameSuffix;
        return this;
    }

    public KafkaStreamsBuilder inputTopic(final String inputTopic) {
        this.inputTopic = inputTopic;
        return this;
    }

    public KafkaStreamsBuilder outputTopic(final String outputTopic) {
        this.outputTopic = outputTopic;
        return this;
    }

    public KafkaStreamsBuilder bootstrapServers(final String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    /**
     * Sets the Kafka Streams property for the number of threads (num.stream.threads). Can be minus
     * one for using the default.
     */
    public KafkaStreamsBuilder numThreads(final int numThreads) {
        if (numThreads < -1 || numThreads == 0) {
            throw new IllegalArgumentException("Number of threads must be greater 0 or -1.");
        }
        this.numThreads = numThreads;
        return this;
    }

    /**
     * Sets the Kafka Streams property for the frequency with which to save the position (offsets in
     * source topics) of tasks (commit.interval.ms). Must be zero for processing all record, for
     * example, when processing bulks of records. Can be minus one for using the default.
     */
    public KafkaStreamsBuilder commitIntervalMs(final int commitIntervalMs) {
        if (commitIntervalMs < -1) {
            throw new IllegalArgumentException("Commit interval must be greater or equal -1.");
        }
        this.commitIntervalMs = commitIntervalMs;
        return this;
    }

    /**
     * Sets the Kafka Streams property for maximum number of memory bytes to be used for record caches
     * across all threads (cache.max.bytes.buffering). Must be zero for processing all record, for
     * example, when processing bulks of records. Can be minus one for using the default.
     */
    public KafkaStreamsBuilder cacheMaxBytesBuffering(final int cacheMaxBytesBuffering) {
        if (cacheMaxBytesBuffering < -1) {
            throw new IllegalArgumentException("Cache max bytes buffering must be greater or equal -1.");
        }
        this.cacheMaxBytesBuff = cacheMaxBytesBuffering;
        return this;
    }

    /**
     * Builds the {@link KafkaStreams} instance.
     */
    public KafkaStreams build() {
        Objects.requireNonNull(this.inputTopic, "Input topic has not been set.");
        Objects.requireNonNull(this.outputTopic, "Output topic has not been set.");
        Objects.requireNonNull(this.cassandraSession, "Cassandra session has not been set.");
        final TopologyBuilder topologyBuilder = new TopologyBuilder(
                this.inputTopic,
                this.outputTopic,
                this.cassandraSession,
                this.tableNameSuffix,
                this.anomalyDetection);
        final Properties properties = PropertiesBuilder
                .bootstrapServers(this.bootstrapServers)
                .applicationId(APPLICATION_NAME + '-' + APPLICATION_VERSION)
                .set(StreamsConfig.NUM_STREAM_THREADS_CONFIG, this.numThreads, p -> p > 0)
                .set(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, this.commitIntervalMs, p -> p >= 0)
                .set(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, this.cacheMaxBytesBuff, p -> p >= 0)
                .build();
        return new KafkaStreams(topologyBuilder.build(), properties);
    }
}
