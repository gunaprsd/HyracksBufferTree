package org.apache.hyracks.storage.am.buffertree.api;

import com.google.common.hash.BloomFilter;

public interface IBloomFilterFactory {

    BloomFilter createEmptyBloomFilter();

    int getSize();

}
