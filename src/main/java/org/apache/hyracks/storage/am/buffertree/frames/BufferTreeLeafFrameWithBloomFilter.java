package org.apache.hyracks.storage.am.buffertree.frames;

import com.google.common.hash.BloomFilter;
import org.apache.hyracks.storage.am.buffertree.api.*;
import org.apache.hyracks.storage.am.buffertree.api.frames.ITreeLeafFrame;
import org.apache.hyracks.storage.am.buffertree.api.frames.ITreeLeafFrameFactory;
import org.apache.hyracks.storage.am.buffertree.api.frames.ITreeLeafFrameWithBloomFilter;
import org.apache.hyracks.storage.am.buffertree.api.frames.ITupleManagerWithBloomFilter;
import org.apache.hyracks.storage.am.buffertree.impl.SlottedTupleManagerWithBloomFilter;


public class BufferTreeLeafFrameWithBloomFilter extends BufferTreeLeafFrame implements ITreeLeafFrameWithBloomFilter {

    protected BufferTreeLeafFrameWithBloomFilter(int leafSize, ITupleWriter writer, ITupleComparator comparator, IBloomFilterFactory bloomFilterFactory) {
        super(leafSize);
        this.tupleManager = new SlottedTupleManagerWithBloomFilter(bloomFilterFactory, this, getPageHeaderSize(), getLeafCapacity(), writer, comparator);
        this.frameTuple = writer.createNewTuple();
        this.frameIndexTuple = writer.createNewIndexTuple();
    }

    public BloomFilter getBloomFilter() {
        return ((ITupleManagerWithBloomFilter)tupleManager).getBloomFilter();
    }

    public static class Factory extends BufferTreeLeafFrame.Factory implements ITreeLeafFrameFactory {

        protected IBloomFilterFactory bloomFilterFactory;

        public Factory(int leafSize, ITupleWriter writer, ITupleComparator comparator, IBloomFilterFactory bfFactory) {
            super(leafSize, writer, comparator);
            this.bloomFilterFactory = bfFactory;

        }
        public ITreeLeafFrame createFrame() {
            return new BufferTreeLeafFrameWithBloomFilter(leafSize, writer, comparator, bloomFilterFactory);
        }
    }

}
