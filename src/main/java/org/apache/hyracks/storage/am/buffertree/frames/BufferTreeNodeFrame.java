package org.apache.hyracks.storage.am.buffertree.frames;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.*;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleReference;
import org.apache.hyracks.storage.am.buffertree.api.frames.ITreeNodeFrame;
import org.apache.hyracks.storage.am.buffertree.api.frames.ITreeNodeFrameFactory;
import org.apache.hyracks.storage.am.buffertree.impl.FindTupleMode;
import org.apache.hyracks.storage.am.buffertree.impl.FrameOpStatus;
import org.apache.hyracks.storage.am.buffertree.impl.MergeTupleIterator;
import org.apache.hyracks.storage.am.buffertree.impl.SlottedTupleManager;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

import java.util.ArrayList;

public class BufferTreeNodeFrame extends AbstractBufferTreeIndexFrame implements ITreeNodeFrame {

    protected int nodeSize;
    protected SlottedTupleManager tupleManager = null;

    public BufferTreeNodeFrame(int nodeSize, ITupleWriter writer, ITupleComparator comparator) {
        this.nodeSize = nodeSize;
        this.frameTuple = writer.createNewTuple();
        this.frameIndexTuple = writer.createNewIndexTuple();
        this.tupleManager = new SlottedTupleManager(this, getPageHeaderSize(), nodeSize, writer, comparator);
    }

    public void setPage(ICachedPage page) {
        super.setPage(page);
    }

    public void initBuffer(byte level) {
        if (level == (byte)0) throw new AssertionError();
        if(level == (byte)1) {
            setType(leafNodeType);
        } else {
            setType(interiorNodeType);
        }
        setLevel(level);
        setSmFlag(false);
        setPageLsn(0);
        setNextSiblingId(-1);
        setPreviousSiblingId(-1);
        tupleManager.initParams();
    }

    public ISlottedTupleManager getIndexTupleManager() {
        return tupleManager;
    }

    public int getChildrenCount() {
        return tupleManager.getTupleCount();
    }

    public int getNodeFreeSpace() {
        return tupleManager.getFreeSpace();
    }

    public int getNodeCapacity() {
        return nodeSize;
    }

    public int getNodeDataSize() {
        int totalSize = nodeSize;
        totalSize -= getPageHeaderSize();
        totalSize -= getNodeFreeSpace();
        totalSize -= tupleManager.getHeaderSize();
        return totalSize;
    }

    public void resetNodeSpaceParams() {
        tupleManager.resetParams();
    }

    public void resetNodeState() {
        tupleManager.resetState();
    }

    public IOrderedTupleIterator getChildrenIterator() throws HyracksDataException {
        return tupleManager.getOrderedIterator();
    }

    public IBulkTupleInserter getNodeBulkInserter() {
        return tupleManager.getBulkInserter();
    }

    public int getChildPageId(ITupleReference searchTuple) throws HyracksDataException {
        int index = tupleManager.findTupleIndex(searchTuple, FindTupleMode.LESS_THAN_EQUALS);
        if (index < 0) throw new AssertionError();
        frameIndexTuple.reset(getBuffer(), tupleManager.getTupleOffsetByIndex(index));
        int pageId = frameIndexTuple.getPageId();
        return pageId;
    }

    public IIndexTupleReference getChildIndexTuple(ITupleReference searchTuple) throws HyracksDataException {
        int index = tupleManager.findTupleIndex(searchTuple, FindTupleMode.LESS_THAN_EQUALS);
        if (index < 0) throw new AssertionError();
        frameIndexTuple.reset(getBuffer(), tupleManager.getTupleOffsetByIndex(index));
        return frameIndexTuple;
    }

    public FrameOpStatus insertIntoNode(ITupleReference tuple) throws HyracksDataException {
        return tupleManager.insertTuple(tuple);
    }

    public void distributeNode(int splitSize, ITreeNodeFrame leftFrame, ITreeNodeFrame rightFrame, IIndexTupleReference splitKey) throws HyracksDataException {
        ITupleIterator childrenIterator = getChildrenIterator();

        split(splitSize, childrenIterator, leftFrame.getNodeBulkInserter(), rightFrame.getNodeBulkInserter(), splitKey);
    }

    public void distributeNode(int splitSize, ArrayList<? extends ITreeNodeFrame> nextFrames, ArrayList<? extends IIndexTupleReference> splitKeys) throws HyracksDataException {
        if (splitKeys.size() != nextFrames.size() - 1) throw new AssertionError();

        ITupleIterator childrenIterator = getChildrenIterator();

        ArrayList<IBulkTupleInserter> inserters = new ArrayList<IBulkTupleInserter>();
        for(int i = 0; i < nextFrames.size(); i++) {
            inserters.add(nextFrames.get(i).getNodeBulkInserter());
        }

        split(splitSize, childrenIterator, inserters, splitKeys);
    }

    public void distributeNodeAndInsert(int splitSize, IOrderedTupleIterator iterator, ArrayList<? extends ITreeNodeFrame> splitFrames, ArrayList<? extends IIndexTupleReference> splitKeys) throws HyracksDataException {
        ITupleIterator mergeIter = new MergeTupleIterator(getChildrenIterator(), iterator, tupleManager.getComparator(), tupleManager.getWriter());

        ArrayList<IBulkTupleInserter> inserters = new ArrayList<IBulkTupleInserter>();
        for(int i = 0; i < splitFrames.size(); i++) {
            inserters.add(splitFrames.get(i).getNodeBulkInserter());
        }

        split(splitSize, mergeIter, inserters, splitKeys);
    }

    public static class Factory implements ITreeNodeFrameFactory {

        int size;
        ITupleWriter writer;
        ITupleComparator comparator;

        public Factory(int size, ITupleWriter writer, ITupleComparator comparator) {
            this.size = size;
            this.writer = writer;
            this.comparator = comparator;
        }

        public ITreeNodeFrame createFrame() {
            return new BufferTreeNodeFrame(size, writer, comparator);
        }
    }

}
