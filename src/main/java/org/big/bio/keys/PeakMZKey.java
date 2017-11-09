package org.big.bio.keys;


/**
 * key represents charge clustering and precursor mz
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @author Yasset Perez-Riverol
 * @version $Id$
 *
 */
public class PeakMZKey implements IKeyable<PeakMZKey> {

    private final double peakMZ;
    private final double precursorMZ;
    private final String peakMZKey;
    private final String precursorMZKey;

    public PeakMZKey(final double pPeakMZ, final double pPrecursorMZ) {
        peakMZ = pPeakMZ;
        precursorMZ = pPrecursorMZ;
        peakMZKey = KeyUtilities.mzToKey(getPeakMZ());
        precursorMZKey = KeyUtilities.mzToKey(getPrecursorMZ());
    }

    public PeakMZKey(String str) {
        final String[] split = str.split(":");
        peakMZ = KeyUtilities.keyToMZ(split[0]);
        precursorMZ = KeyUtilities.keyToMZ(split[1]);
        peakMZKey = KeyUtilities.mzToKey(getPeakMZ());
        precursorMZKey = KeyUtilities.mzToKey(getPrecursorMZ());
    }

    /**
     * MZ_RESOLUTION * peakMZ
     * @return peakMZ Value
     */
    public double getPeakMZ() {
        return peakMZ;
    }

    /**
     * MZ_RESOLUTION * getPrecursorMZ
     *
     * @return precursorMZ Value
     */
    public double getPrecursorMZ() {
        return precursorMZ;
    }

    @Override
    public String toString() {
        return peakMZKey + ":" + precursorMZKey;
    }

    @Override
    public boolean equals(final Object o) {
        return o != null && getClass() == o.getClass() && toString().equals(o.toString());
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }


    /**
     * sort by string works
     *
     * @param o PeakMzKey to compare
     * @return int value result of the comparison
     */
    @Override
    public int compareTo(final PeakMZKey o) {
        if(o != null)
            return toString().compareTo(o.toString());
        return 1;
    }

    /**
     * here is an int that a partitioner would use
     *
     * @return Partition for the corresponding peakMZ
     */
    public int getPartitionHash() {
        return Math.abs(peakMZKey.hashCode());
    }
}
