package org.apache.hyracks.storage.am.buffertree.api.frames;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.IBulkTupleInserter;
import org.apache.hyracks.storage.am.buffertree.api.IOrderedTupleIterator;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleReference;
import org.apache.hyracks.storage.am.buffertree.impl.FrameOpStatus;

import java.util.ArrayList;

public interface ITreeLeafFrame extends ITreeIndexFrame {

    int getLeafTupleCount();
    int getLeafFreeSpace();
    int getLeafCapacity();
    int getLeafDataSize();
    void resetLeafSpaceParams();
    void resetLeafState();


    FrameOpStatus insertIntoLeaf(IOrderedTupleIterator iterator) throws HyracksDataException;
    FrameOpStatus insertIntoLeaf(ITupleReference tuple) throws HyracksDataException;

    void distributeAndInsert(int splitSize, IOrderedTupleIterator iterator, ArrayList<? extends ITreeLeafFrame> splitFrames, ArrayList<? extends IIndexTupleReference> splitKeys) throws HyracksDataException;
    void distribute(int splitSize, ITreeLeafFrame leftFrame, ITreeLeafFrame rightFrame, IIndexTupleReference splitKey) throws HyracksDataException;
    IOrderedTupleIterator getLeafIterator();
    IBulkTupleInserter getLeafBulkInserter();

    ITupleReference getMatchingKeyTuple(ITupleReference searchTuple) throws HyracksDataException;

}
