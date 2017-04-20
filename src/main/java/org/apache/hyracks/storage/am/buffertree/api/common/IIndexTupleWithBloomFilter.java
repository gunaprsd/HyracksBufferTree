package org.apache.hyracks.storage.am.buffertree.api.common;

import com.google.common.hash.BloomFilter;

public interface IIndexTupleWithBloomFilter extends IIndexTupleReference {

    void setBloomFilter(BloomFilter bf);

    void addBloomFilter(BloomFilter bf);

    BloomFilter getBloomFilter();
}
