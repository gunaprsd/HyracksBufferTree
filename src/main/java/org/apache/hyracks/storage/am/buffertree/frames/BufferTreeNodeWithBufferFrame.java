package org.apache.hyracks.storage.am.buffertree.frames;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.*;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleReference;
import org.apache.hyracks.storage.am.buffertree.api.frames.ITreeNodeWithBufferFrame;
import org.apache.hyracks.storage.am.buffertree.api.frames.ITreeNodeWithBufferFrameFactory;
import org.apache.hyracks.storage.am.buffertree.common.KeyValueTuple;
import org.apache.hyracks.storage.am.buffertree.impl.FrameOpStatus;
import org.apache.hyracks.storage.am.buffertree.impl.SlottedTupleManager;
import org.apache.hyracks.storage.am.buffertree.impl.UnslottedTupleManager;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

import java.util.ArrayList;

/**
 * Created by t-guje on 11/3/2015.
 */
public class BufferTreeNodeWithBufferFrame extends BufferTreeNodeFrame implements ITreeNodeWithBufferFrame {
    protected int bufferSize;
    protected ITupleManager bufferTupleManager;

    public BufferTreeNodeWithBufferFrame(int nodeSize, int bufferSize, ITupleWriter nodeWriter, ITupleWriter bufferWriter, ITupleComparator comparator, boolean slotted) {
        super(nodeSize, nodeWriter, comparator);
        this.bufferSize = bufferSize;
        if(slotted) {
            this.bufferTupleManager = new SlottedTupleManager(this, nodeSize, nodeSize + bufferSize, bufferWriter, comparator);
        } else {
            this.bufferTupleManager = new UnslottedTupleManager(this, nodeSize, nodeSize + bufferSize, bufferWriter, comparator);
        }
    }

    public void setPage(ICachedPage page) {
        super.setPage(page);
        bufferTupleManager.resetState();
    }

    public void initBuffer(byte level) {
        super.initBuffer(level);
        bufferTupleManager.initParams();
    }

    public int getBufferTupleCount() {
        return bufferTupleManager.getTupleCount();
    }

    public int getBufferFreeSpace() {
        return bufferTupleManager.getFreeSpace();
    }

    public int getBufferCapacity() {
        return bufferSize;
    }

    public void resetBufferSpaceParams() {
        bufferTupleManager.resetParams();
    }

    public void resetBufferState() {
        bufferTupleManager.resetState();
    }

    public IOrderedTupleIterator getBufferIterator() throws HyracksDataException {
        return bufferTupleManager.getOrderedIterator();
    }

    public IBulkTupleInserter getBufferBulkInserter() {
        return bufferTupleManager.getBulkInserter();
    }

    public FrameOpStatus insertIntoBuffer(ITupleReference tuple) throws HyracksDataException {
        return bufferTupleManager.insertTuple(tuple);
    }

    public FrameOpStatus insertIntoBuffer(ITupleIterator iterator) throws HyracksDataException {
        FrameOpStatus status = FrameOpStatus.FAIL;
        while(iterator.hasNext()) {
            if(insertIntoBuffer(iterator.peek()) == FrameOpStatus.COMPLETED) {
                status = FrameOpStatus.PARTIALLY_COMPLETED;
                iterator.next();
            } else {
                return status;
            }
        }
        status = FrameOpStatus.COMPLETED;
        return status;
    }

    public ITupleReference getMatchingKeyInBuffer(ITupleReference searchKey) throws HyracksDataException {
        return bufferTupleManager.getMatchingKeyTuple(searchKey);
    }

    public void resetSize(int nodeSize, int bufferSize) {
        this.bufferSize = bufferSize;
        this.nodeSize = nodeSize;
        this.tupleManager.resetSizeOffsets(getPageHeaderSize(), nodeSize);
        this.bufferTupleManager.resetSizeOffsets(nodeSize, nodeSize + bufferSize);
    }

    public void distributeNodeAndBuffer(int splitSize, ArrayList<? extends ITreeNodeWithBufferFrame> splitFrames, ArrayList<? extends IIndexTupleReference> splitKeys) throws HyracksDataException {
        distributeNode(splitSize, splitFrames, splitKeys);

        IOrderedTupleIterator bufferIterator = getBufferIterator();
        for(int i = 0; i < splitFrames.size(); i++) {
            if(i < splitKeys.size()) {
                bufferIterator.setEndLimit(splitKeys.get(i));
            } else {
                bufferIterator.setEndLimit(KeyValueTuple.HIGHEST_KEY_TUPLE);
            }
            splitFrames.get(i).insertIntoBuffer(bufferIterator);
        }

        if (!bufferIterator.done()) throw new AssertionError();
    }

    public void distributeNodeAndBuffer(int splitSize, ITreeNodeWithBufferFrame leftFrame, ITreeNodeWithBufferFrame rightFrame, IIndexTupleReference splitKey) throws HyracksDataException {
        distributeNode(splitSize, leftFrame, rightFrame, splitKey);

        IOrderedTupleIterator bufferIterator = getBufferIterator();
        bufferIterator.setEndLimit(splitKey);
        leftFrame.insertIntoBuffer(bufferIterator);
        bufferIterator.setEndLimit(KeyValueTuple.HIGHEST_KEY_TUPLE);
        rightFrame.insertIntoBuffer(bufferIterator);

        if (!bufferIterator.done()) throw new AssertionError();
    }

}
