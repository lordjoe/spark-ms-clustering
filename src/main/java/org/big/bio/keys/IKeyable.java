package org.big.bio.keys;

import java.io.Serializable;

/**
 * Key interface to make a key partitionable, comparable and serializable
 *
 * @author Rui Wang
 * @version $Id$
 */
public interface IKeyable<T extends IKeyable> extends Comparable<T>, Serializable, IPartitionable{

}
