package org.big.bio.keys;

/**
 * Implemented by an object with knowledge about its own partition
 */
public interface IPartitionable {

    /**
     * here is an int that a partitioner would use
     *
     * @return
     */
    int getPartitionHash();

}
