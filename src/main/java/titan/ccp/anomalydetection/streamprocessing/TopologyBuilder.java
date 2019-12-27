package titan.ccp.anomalydetection.streamprocessing;

import com.datastax.driver.core.Session;
import kieker.common.record.IMonitoringRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public final class TopologyBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyBuilder.class);

    private final String inputTopic;
    private final String outputTopic;
    private final Session cassandraSession;
    private final IAnomalyDetection anomalyDetection;

    private final StreamsBuilder builder = new StreamsBuilder();

    /**
     * Create a new {@link TopologyBuilder} using the given topics.
     */
    public TopologyBuilder(final String inputTopic, final String outputTopic, final Session cassandraSession) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.cassandraSession = cassandraSession;
        this.anomalyDetection = new AnomalyDetection();
    }

    /**
     * Build the {@link Topology} for the Anomaly-Detection microservice.
     */
    public Topology build() {
        final CassandraWriter<IMonitoringRecord> normalCassandraWriter
                = buildCassandraWriter(ActivePowerRecord.class);
        final CassandraWriter<IMonitoringRecord> aggregatedCassandraWriter
                = buildCassandraWriter(AggregatedActivePowerRecord.class);
        this.builder
                .stream(this.inputTopic, Consumed.with(
                        Serdes.String(),
                        IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())))
                // TODO Logging
//                .peek((k, record) -> LOGGER.info("Write ActivePowerRecord to Cassandra {}",
//                        this.buildActivePowerRecordString(record)))
                .filter(anomalyDetection.activePowerRecordAnomalyDetection()::test)
                .peek((key, record) -> System.out.println("Anomaly detected! Writing record: "+buildActivePowerRecordString(record)+" to Cassandra"));
                //.foreach((key, record) -> // todo: write to cassandra (in correct table)!);

        this.builder
                .stream(this.outputTopic, Consumed.with(
                        Serdes.String(),
                        IMonitoringRecordSerde.serde(new AggregatedActivePowerRecordFactory())))
                // TODO Logging
//                .peek((k, record) -> LOGGER.info("Write AggregatedActivePowerRecord to Cassandra {}",
//                        this.buildAggActivePowerRecordString(record)))
                .filter(anomalyDetection.aggregatedActivePowerRecordAnomalyDetection()::test)
                .peek((key, record) -> System.out.println(buildAggActivePowerRecordString(record)));

        return this.builder.build();
    }

    private CassandraWriter<IMonitoringRecord> buildCassandraWriter(
            final Class<? extends IMonitoringRecord> recordClass) {
            final ExplicitPrimaryKeySelectionStrategy primaryKeySelectionStrategy =
                new ExplicitPrimaryKeySelectionStrategy();
        primaryKeySelectionStrategy.registerPartitionKeys(recordClass.getSimpleName(), "identifier");
        primaryKeySelectionStrategy.registerClusteringColumns(recordClass.getSimpleName(), "timestamp");

        final CassandraWriter<IMonitoringRecord> cassandraWriter =
                CassandraWriter.builder(this.cassandraSession, new KiekerDataAdapter())
                        .tableNameMapper(PredefinedTableNameMappers.SIMPLE_CLASS_NAME)
                        .primaryKeySelectionStrategy(primaryKeySelectionStrategy).build();

        return cassandraWriter;
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
