package titan.ccp.anomalydetection.api.server;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import kieker.common.record.factory.IRecordFactory;

import titan.ccp.common.kieker.cassandra.CassandraDeserializer;
import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.models.records.ActivePowerRecordFactory;
import titan.ccp.models.records.AggregatedActivePowerRecord;
import titan.ccp.models.records.AggregatedActivePowerRecordFactory;

/**
 * A proxy class to encapsulate the database and queries to it.
 * (This class is almost identical to the Repository from the History-Microservice)
 *
 * @param <T> type of records in this repository
 */
public class ActivePowerRepository<T> {

    private static final String TIMESTAMP_KEY = "timestamp";
    private static final String IDENTIFIER_KEY = "identifier";

    private final Session cassandraSession;
    private final String tableName;
    private final Function<Row, T> recordFactory;
    private final ToDoubleFunction<T> valueAccessor;

    /**
     * Create a new {@link ActivePowerRepository}.
     */
    public ActivePowerRepository(final Session cassandraSession, final String tableName,
                                 final Function<Row, T> recordFactory, final ToDoubleFunction<T> valueAccessor) {
        this.cassandraSession = cassandraSession;
        this.tableName = tableName;
        this.recordFactory = recordFactory;
        this.valueAccessor = valueAccessor;
    }

    /**
     * Create a new {@link ActivePowerRepository}.
     */
    public ActivePowerRepository(final Session cassandraSession, final String tableName,
                                 final IRecordFactory<T> recordFactory, final ToDoubleFunction<T> valueAccessor) {
        this(cassandraSession,
            tableName,
            row -> recordFactory.create(new CassandraDeserializer(row, recordFactory.getValueNames())),
            valueAccessor);
    }

    /**
     * Get all selected records.
     */
    public List<T> get(final String identifier, final long after) {
        final Statement statement = QueryBuilder.select().all().from(this.tableName)
                .where(QueryBuilder.eq(IDENTIFIER_KEY, identifier))
                .and(QueryBuilder.gt(TIMESTAMP_KEY, after));

        return this.get(statement);
    }

    private List<T> get(final Statement statement) {
        final ResultSet resultSet = this.cassandraSession.execute(statement); // NOPMD no close()

        final List<T> records = new ArrayList<>();
        for (final Row row : resultSet) {
            final T record = this.recordFactory.apply(row);
            records.add(record);
        }

        return records;
    }

    /**
     * Get the latests records.
     */
    public List<T> getLatest(final String identifier, final int count) {
        final Statement statement = QueryBuilder.select().all()
                .from(this.tableName)
                .where(QueryBuilder.eq(IDENTIFIER_KEY, identifier))
                .orderBy(QueryBuilder.desc(TIMESTAMP_KEY))
                .limit(count);

        return this.get(statement);
    }

    /**
     * Get a frequency distribution of records. Records are grouped by their values and this methods
     * returns a list of {@link DistributionBucket}s.
     */
    public List<DistributionBucket> getDistribution(final String identifier, final long after,
                                                    final int bucketsCount) {
        final List<T> records = this.get(identifier, after);

        if (records.isEmpty()) {
            return Collections.emptyList();
        }

        final double min = records.stream().mapToDouble(this.valueAccessor).min().getAsDouble();
        final double max = records.stream().mapToDouble(this.valueAccessor).max().getAsDouble();

        final double sliceSize = (max - min) / bucketsCount;

        final int[] distribution = new int[bucketsCount];
        for (final T record : records) {
            final double value = this.valueAccessor.applyAsDouble(record);
            final int index = Integer.min((int) ((value - min) / sliceSize), bucketsCount - 1);
            distribution[index]++;
        }

        final List<DistributionBucket> buckets = new ArrayList<>(bucketsCount);
        for (int i = 0; i < bucketsCount; i++) {
            final double lower = i > 0 ? buckets.get(i - 1).getUpper() : min;
            final double upper = i < bucketsCount ? lower + sliceSize : max;
            buckets.add(new DistributionBucket(lower, upper, distribution[i])); // NOPMD
        }

        return buckets;
    }

    /**
     * Get the total amount of all records.
     */
    public long getTotalCount() {
        // TODO This is not working for huge data sets
        final Statement statement = QueryBuilder.select().countAll().from(this.tableName);
        return this.cassandraSession.execute(statement).all().get(0).getLong(0);
    }

    /**
     * Get the number of records for the given sensor identifier and after a timestamp.
     */
    public long getCount(final String identifier, final long after) {
        final Statement statement = QueryBuilder.select().countAll().from(this.tableName)
                .where(QueryBuilder.eq(IDENTIFIER_KEY, identifier))
                .and(QueryBuilder.gt(TIMESTAMP_KEY, after));
        return this.cassandraSession.execute(statement).all().get(0).getLong(0);
    }

    /**
     * Get all available sensor identifiers.
     */
    public List<String> getIdentifiers() {
        final Statement statement = QueryBuilder.select(IDENTIFIER_KEY).distinct().from(this.tableName);
        return this.cassandraSession.execute(statement).all().stream().map(row -> row.getString(0))
                .collect(Collectors.toList());
    }

    /**
     * Create an {@link ActivePowerRepository} for {@link AggregatedActivePowerRecord}s.
     */
    public static ActivePowerRepository<AggregatedActivePowerRecord> forAggregated(
            final Session cassandraSession, final String tableNameSuffix) {
        return new ActivePowerRepository<>(
            cassandraSession,
            AggregatedActivePowerRecord.class.getSimpleName() + tableNameSuffix,
            new AggregatedActivePowerRecordFactory(),
            record -> record.getSumInW()
        );
    }

    /**
     * Create an {@link ActivePowerRepository} for {@link ActivePowerRecord}s.
     */
    public static ActivePowerRepository<ActivePowerRecord> forNormal(final Session cassandraSession,
                                                                     final String tableNameSuffix) {
        return new ActivePowerRepository<>(
            cassandraSession,
            ActivePowerRecord.class.getSimpleName() + tableNameSuffix,
            new ActivePowerRecordFactory(),
            record -> record.getValueInW()
        );
    }

}
