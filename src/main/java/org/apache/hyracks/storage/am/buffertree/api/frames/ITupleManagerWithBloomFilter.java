package org.apache.hyracks.storage.am.buffertree.api.frames;

import com.google.common.hash.BloomFilter;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface ITupleManagerWithBloomFilter {

    BloomFilter getBloomFilter();

    void putInBloomFilter(ITupleReference keyValue);

    void resetBloomFilter();
}
