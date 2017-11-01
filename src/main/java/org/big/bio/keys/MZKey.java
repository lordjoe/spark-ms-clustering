package org.big.bio.keys;


import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;

/**
 * The precursor MZKey is used to sort precursors by the corresponding Mz Value.
 * <p/>
 */
public class MZKey implements IKeyable<MZKey> {

    private final double precursorMZ;
    private final String precursorMZKey;

    public MZKey(final double pPrecursorMZ) {
        precursorMZ = pPrecursorMZ;
        precursorMZKey = KeyUtilities.mzToKey(getPrecursorMZ());
    }

    public MZKey(String str) {
        precursorMZ = KeyUtilities.keyToMZ(str);
        precursorMZKey = KeyUtilities.mzToKey(getPrecursorMZ());
    }

    public double getPrecursorMZ() {
        return precursorMZ;
    }

    public int getAsInt() {
        return (int) precursorMZ;
    }

    @Override
    public String toString() {
        return precursorMZKey;
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
     * here is an int that a partitioner would use
     *
     * @return
     */
    public int getPartitionHash() {
        return (int) (getPrecursorMZ() * MZIntensityUtilities.MZ_RESOLUTION + 0.5);
    }


    /**
     * sort by string works
     *
     * @param o
     * @return
     */
    @Override
    public int compareTo(final MZKey o) {
        return toString().compareTo(o.toString());
    }
}
