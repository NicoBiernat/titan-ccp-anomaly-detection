package titan.ccp.anomalydetection.streamprocessing;

import com.datastax.driver.core.Session;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import kieker.common.record.IMonitoringRecord;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import titan.ccp.anomalydetection.streamprocessing.detection.IAnomalyDetection;
import titan.ccp.common.cassandra.CassandraWriter;
import titan.ccp.common.cassandra.ExplicitPrimaryKeySelectionStrategy;
import titan.ccp.common.cassandra.PredefinedTableNameMappers;
import titan.ccp.common.kieker.cassandra.KiekerDataAdapter;
import titan.ccp.common.kieker.kafka.IMonitoringRecordSerde;
import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.models.records.ActivePowerRecordFactory;
import titan.ccp.models.records.AggregatedActivePowerRecord;
import titan.ccp.models.records.AggregatedActivePowerRecordFactory;


/**
 * Builds Kafka Stream Topology for the Anomaly-Detection microservice.
 */
public class TopologyBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyBuilder.class);

    private final String inputTopic;
    private final String outputTopic;
    private final Session cassandraSession;
    private final String tableNameSuffix;
    private final IAnomalyDetection anomalyDetection;

    private final StreamsBuilder builder = new StreamsBuilder();

    /**
     * Create a new {@link TopologyBuilder} using the given topics.
     */
    public TopologyBuilder(final String inputTopic, final String outputTopic,
                           final Session cassandraSession, final String tableNameSuffix,
                           final IAnomalyDetection anomalyDetection) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.cassandraSession = cassandraSession;
        this.tableNameSuffix = tableNameSuffix;
        this.anomalyDetection = anomalyDetection;
    }

    /**
     * Build the {@link Topology} for the Anomaly-Detection microservice.
     */
    public Topology build() {
        final CassandraWriter<IMonitoringRecord> normalCassandraWriter
                = buildCassandraWriter(ActivePowerRecord.class);
        final CassandraWriter<IMonitoringRecord> aggregatedCassandraWriter
                = buildCassandraWriter(AggregatedActivePowerRecord.class);
        createTableIfNotExists(normalCassandraWriter,
                new ActivePowerRecord("",0,0));
        createTableIfNotExists(aggregatedCassandraWriter,
                new AggregatedActivePowerRecord("",0,0,0,0,0,0));
        this.builder
                .stream(this.inputTopic, Consumed.with(
                        Serdes.String(),
                        IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())))
                .filter(anomalyDetection.activePowerRecordAnomalyDetection()::test)
                .peek((key, record) ->
                        LOGGER.info("Anomaly detected! Writing ActivePowerRecord {} to Cassandra\n",
                                    buildActivePowerRecordString(record)))
                .foreach((key, record) -> normalCassandraWriter.write(record));

        this.builder
                .stream(this.outputTopic, Consumed.with(
                        Serdes.String(),
                        IMonitoringRecordSerde.serde(new AggregatedActivePowerRecordFactory())))
                .filter(anomalyDetection.aggregatedActivePowerRecordAnomalyDetection()::test)
                .peek((key, record) ->
                        LOGGER.info("Anomaly detected! Writing AggregatedActivePowerRecord {} to Cassandra\n",
                                    buildAggActivePowerRecordString(record)))
                .foreach((key, record) -> aggregatedCassandraWriter.write(record));

        return this.builder.build();
    }

    private CassandraWriter<IMonitoringRecord> buildCassandraWriter(
            final Class<? extends IMonitoringRecord> recordClass) {
        final ExplicitPrimaryKeySelectionStrategy primaryKeySelectionStrategy =
                new ExplicitPrimaryKeySelectionStrategy();
        primaryKeySelectionStrategy.registerPartitionKeys(
                recordClass.getSimpleName() + tableNameSuffix, "identifier");
        primaryKeySelectionStrategy.registerClusteringColumns(
                recordClass.getSimpleName() + tableNameSuffix, "timestamp");

        return CassandraWriter.builder(this.cassandraSession, new KiekerDataAdapter())
                .tableNameMapper(r -> PredefinedTableNameMappers.SIMPLE_CLASS_NAME.apply(r) + tableNameSuffix)
                .primaryKeySelectionStrategy(primaryKeySelectionStrategy)
                .build();
    }

    /*
     * If the tables don't exist, the API will throw an error when trying to access them.
     * This is a really inappropriate way to handle this but the CassandraWriter does not create tables when started
     * but only when written to.
     * And the createTableIfNotExists method is private but I can't change the titan-ccp-commons library.
     */
    private void createTableIfNotExists(final CassandraWriter<IMonitoringRecord> cassandraWriter,
                                        final IMonitoringRecord record) {
        try {
            final Method createTableIfNotExists = cassandraWriter.getClass()
                    .getDeclaredMethod("createTableIfNotExists", String.class, Object.class);
            createTableIfNotExists.setAccessible(true);
            createTableIfNotExists.invoke(cassandraWriter,
                    record.getClass().getSimpleName() + tableNameSuffix, record);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            LOGGER.warn(e.getStackTrace().toString());
        }
    }

    private String buildActivePowerRecordString(final ActivePowerRecord record) {
        return "{" + record.getIdentifier() + ';' + record.getTimestamp() + ';' + record.getValueInW()
                + '}';
    }

    private String buildAggActivePowerRecordString(final AggregatedActivePowerRecord record) {
        return "{" + record.getIdentifier() + ';' + record.getTimestamp() + ';' + record.getSumInW()
                + '}';
    }
}
