package org.apache.hyracks.storage.am.buffertree.impl;

import com.google.common.hash.BloomFilter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.*;
import org.apache.hyracks.storage.am.buffertree.api.frames.ICachedPageFrame;
import org.apache.hyracks.storage.am.buffertree.api.frames.ITupleManagerWithBloomFilter;
import org.apache.hyracks.storage.am.buffertree.common.KeyValueTuple;

/**
 * Created by t-guje on 10/26/2015.
 */
public class SlottedTupleManagerWithBloomFilter extends SlottedTupleManager implements ITupleManagerWithBloomFilter {

    //frame variable
    protected final IBloomFilterFactory bloomFilterFactory;

    //state variable
    protected BloomFilter bloomFilter;

    /**
     * Constructor for a slotted tuple manage with bloom filter
     *
     * @param frame       underlying frame for the manager
     * @param startOffset start offset of the space that the manager has to manage
     * @param endOffset   endOff offset of the space that the manager has to manage
     * @param writer      writer used to write a tuple
     * @param comparator  comparator used for comparing tuples
     */
    public SlottedTupleManagerWithBloomFilter(IBloomFilterFactory bloomFilterFactory, ICachedPageFrame frame, int startOffset, int endOffset, ITupleWriter writer, ITupleComparator comparator) {
        super(frame, startOffset, endOffset, writer, comparator);
        this.bloomFilterFactory = bloomFilterFactory;
        this.bloomFilter = bloomFilterFactory.createEmptyBloomFilter();
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    public void putInBloomFilter(ITupleReference tuple) {
        bloomFilter.put(((KeyValueTuple)tuple).getKey());
    }

    public void resetBloomFilter() {
        bloomFilter = bloomFilterFactory.createEmptyBloomFilter();
    }


    public FrameOpStatus insertTuple(ITupleReference tuple) throws HyracksDataException {
        FrameOpStatus opStatus = super.insertTuple(tuple);
        if(opStatus == FrameOpStatus.COMPLETED) {
            putInBloomFilter(tuple);
        }
        return opStatus;
    }
    public void resetState() {
        super.resetState();
        bloomFilter = bloomFilterFactory.createEmptyBloomFilter();
    }

    /**
     * Must be used only for empty iterators
     * @return
     */
    public IBulkTupleInserter getBulkInserter() {
        if(bulkInserter == null) {
            bulkInserter = new BulkTupleInserter(this);
        } else {
            bulkInserter.reset();
        }
        return bulkInserter;
    }

    public class BulkTupleInserter extends SlottedTupleManager.BulkInserter  {

        public BulkTupleInserter(SlottedTupleManagerWithBloomFilter manager) {
            super(manager);
        }

        public void init() {
            super.init();
            ((SlottedTupleManagerWithBloomFilter)manager).resetBloomFilter();
        }

        public FrameOpStatus insert(ITupleReference tuple) {
            if(super.insert(tuple) == FrameOpStatus.COMPLETED) {
                ((ITupleManagerWithBloomFilter)manager).putInBloomFilter(tuple);
                return FrameOpStatus.COMPLETED;
            } else {
                return FrameOpStatus.FAIL;
            }
        }

        public void reset() {
            ((ITupleManagerWithBloomFilter)manager).resetBloomFilter();
            super.reset();
        }

    }
}
