package org.apache.hyracks.storage.am.buffertree.api.frames;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.IBulkTupleInserter;
import org.apache.hyracks.storage.am.buffertree.api.IOrderedTupleIterator;
import org.apache.hyracks.storage.am.buffertree.api.ITupleManager;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleReference;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleWithBloomFilter;
import org.apache.hyracks.storage.am.buffertree.impl.FrameOpStatus;

import java.util.ArrayList;

public interface ITreeNodeFrame extends ITreeIndexFrame {

    int getChildrenCount();
    int getNodeFreeSpace();
    int getNodeCapacity();
    int getNodeDataSize();
    void resetNodeSpaceParams();
    void resetNodeState();

    int getChildPageId(ITupleReference searchTuple) throws HyracksDataException;
    IIndexTupleReference getChildIndexTuple(ITupleReference searchTuple) throws HyracksDataException;

    FrameOpStatus insertIntoNode(ITupleReference tuple) throws HyracksDataException;
    void distributeNode(int splitSize, ITreeNodeFrame leftFrame, ITreeNodeFrame rightFrame, IIndexTupleReference splitKey) throws HyracksDataException;
    void distributeNode(int splitSize, ArrayList<? extends ITreeNodeFrame> nextFrames, ArrayList<? extends IIndexTupleReference> splitKeys) throws HyracksDataException;
    void distributeNodeAndInsert(int splitSize, IOrderedTupleIterator iterator, ArrayList<? extends ITreeNodeFrame> nextFrames, ArrayList<? extends IIndexTupleReference> splitKeys) throws HyracksDataException;

    IOrderedTupleIterator getChildrenIterator() throws HyracksDataException;
    IBulkTupleInserter getNodeBulkInserter();

}
