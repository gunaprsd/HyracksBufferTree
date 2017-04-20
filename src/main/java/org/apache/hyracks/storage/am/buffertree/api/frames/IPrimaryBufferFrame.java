package org.apache.hyracks.storage.am.buffertree.api.frames;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.IBulkTupleInserter;
import org.apache.hyracks.storage.am.buffertree.api.IOrderedTupleIterator;
import org.apache.hyracks.storage.am.buffertree.api.ITupleIterator;
import org.apache.hyracks.storage.am.buffertree.impl.FrameOpStatus;

public interface IPrimaryBufferFrame extends ICachedPageFrame {

    int getSerialId();

    int getTupleCount();

    int getFreeSpace();

    int getBufferCapacity();

    void reset();

    void initBuffer(int serialId);

    FrameOpStatus insert(ITupleIterator iterator) throws HyracksDataException;

    FrameOpStatus insert(ITupleReference tuple) throws HyracksDataException;

    ITupleIterator getIterator();

    IOrderedTupleIterator getOrderedIterator();

    IBulkTupleInserter getBulkInserter();

    ITupleReference getMatchingKeyTuple(ITupleReference searchTuple) throws HyracksDataException;
}
