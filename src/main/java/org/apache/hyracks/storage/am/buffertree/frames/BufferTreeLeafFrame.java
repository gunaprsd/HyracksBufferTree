package org.apache.hyracks.storage.am.buffertree.frames;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.*;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleReference;
import org.apache.hyracks.storage.am.buffertree.api.frames.ITreeLeafFrame;
import org.apache.hyracks.storage.am.buffertree.api.frames.ITreeLeafFrameFactory;
import org.apache.hyracks.storage.am.buffertree.impl.FrameOpStatus;
import org.apache.hyracks.storage.am.buffertree.impl.MergeTupleIterator;
import org.apache.hyracks.storage.am.buffertree.impl.SlottedTupleManager;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

import java.util.ArrayList;

public class BufferTreeLeafFrame extends AbstractBufferTreeIndexFrame implements ITreeLeafFrame {

    protected int leafSize;
    protected SlottedTupleManager tupleManager = null;
    protected MergeTupleIterator mergedIterator = null;

    protected BufferTreeLeafFrame(int leafSize, ITupleWriter writer, ITupleComparator comparator) {
        this.leafSize = leafSize;
        this.tupleManager = new SlottedTupleManager(this, getPageHeaderSize(), getLeafCapacity(), writer, comparator);
        this.frameTuple = writer.createNewTuple();
        this.frameIndexTuple = writer.createNewIndexTuple();
    }

    protected BufferTreeLeafFrame(int leafSize) {
        this.leafSize = leafSize;
        this.tupleManager = null;
        this.frameTuple = null;
        this.frameIndexTuple = null;
    }

    public void setPage(ICachedPage page) {
        super.setPage(page);
        tupleManager.resetState();
        if(mergedIterator != null)mergedIterator.reset();
    }

    public void initBuffer(byte level) {
        if (level != 0) throw new AssertionError();
        setType(leafType);
        setLevel((byte)0);
        setSmFlag(false);
        setPageLsn(0);
        setNextSiblingId(-1);
        setPreviousSiblingId(-1);
        tupleManager.initParams();
    }

    public ISlottedTupleManager getIndexTupleManager() {
        return tupleManager;
    }

    public int getLeafTupleCount() {
        return tupleManager.getTupleCount();
    }

    public int getLeafFreeSpace() {
        return tupleManager.getFreeSpace();
    }

    public int getLeafCapacity() {
        return leafSize;
    }

    public int getLeafDataSize() {
        int totalSize = leafSize;
        totalSize -= getPageHeaderSize();
        totalSize -= getLeafFreeSpace();
        totalSize -= tupleManager.getHeaderSize();
        return totalSize;
    }

    public ITupleReference getMatchingKeyTuple(ITupleReference searchTuple) throws HyracksDataException {
        return tupleManager.getMatchingKeyTuple(searchTuple);
    }

    public void resetLeafSpaceParams() {
        tupleManager.resetParams();
    }

    public void resetLeafState() {
        tupleManager.resetState();
    }

    public MergeTupleIterator getMergedTupleIterator(IOrderedTupleIterator iterator) throws HyracksDataException {
        if(mergedIterator == null) {
            mergedIterator = new MergeTupleIterator(getLeafIterator(), iterator,
                    tupleManager.getComparator(), tupleManager.getWriter());
        } else {
            mergedIterator.reset(getLeafIterator(), iterator);
        }
        return mergedIterator;
    }

    public IOrderedTupleIterator getLeafIterator() {
        return tupleManager.getOrderedIterator();
    }

    public IBulkTupleInserter getLeafBulkInserter() {
        return tupleManager.getBulkInserter();
    }

    public FrameOpStatus insertIntoLeaf(ITupleReference tuple) throws HyracksDataException {
        return tupleManager.insertTuple(tuple);
    }

    public void distributeAndInsert(int splitSize, IOrderedTupleIterator iterator, ArrayList<? extends ITreeLeafFrame> splitFrames, ArrayList<? extends IIndexTupleReference> splitKeys) throws HyracksDataException {
        ITupleIterator mergeIter = getMergedTupleIterator(iterator);

        ArrayList<IBulkTupleInserter> inserters = new ArrayList<IBulkTupleInserter>();
        for(int i = 0; i < splitFrames.size(); i++) {
            inserters.add(splitFrames.get(i).getLeafBulkInserter());
        }

        split(splitSize, mergeIter, inserters, splitKeys);
    }

    public void distribute(int splitSize, ITreeLeafFrame leftFrame, ITreeLeafFrame rightFrame, IIndexTupleReference splitKey) throws HyracksDataException {
        split(splitSize, getLeafIterator(), leftFrame.getLeafBulkInserter(), rightFrame.getLeafBulkInserter(), splitKey);
    }

    public FrameOpStatus insertIntoLeaf(IOrderedTupleIterator iterator) throws HyracksDataException {
        FrameOpStatus status = FrameOpStatus.FAIL;
        while(iterator.hasNext()) {
            if(insertIntoLeaf(iterator.peek()) == FrameOpStatus.COMPLETED) {
                status = FrameOpStatus.PARTIALLY_COMPLETED;
                iterator.next();
            } else {
                return status;
            }
        }
        status = FrameOpStatus.COMPLETED;
        return status;
    }

    public static class Factory implements ITreeLeafFrameFactory {
        protected ITupleWriter writer;
        protected ITupleComparator comparator;
        protected int leafSize;

        public Factory(int leafSize, ITupleWriter writer, ITupleComparator comparator) {
            this.leafSize = leafSize;
            this.writer = writer;
            this.comparator = comparator;
        }

        public ITreeLeafFrame createFrame() {
            return new BufferTreeLeafFrame(leafSize, writer, comparator);
        }
    }
}

