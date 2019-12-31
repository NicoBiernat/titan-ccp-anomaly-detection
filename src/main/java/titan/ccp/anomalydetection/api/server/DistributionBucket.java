package titan.ccp.anomalydetection.api.server;

/**
 * A distribution bucket stores the number of elements for a range.
 */
public class DistributionBucket {

    private final double lower;
    private final double upper;
    private final int elements;

    /**
     * Create a new {@link DistributionBucket}.
     */
    public DistributionBucket(final double lower, final double upper, final int elements) {
        this.lower = lower;
        this.upper = upper;
        this.elements = elements;
    }

    public double getLower() {
        return this.lower;
    }

    public double getUpper() {
        return this.upper;
    }

    public int getElements() {
        return this.elements;
    }

}