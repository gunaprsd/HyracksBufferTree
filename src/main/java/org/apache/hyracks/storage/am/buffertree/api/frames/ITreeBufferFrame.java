package org.apache.hyracks.storage.am.buffertree.api.frames;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.IBulkTupleInserter;
import org.apache.hyracks.storage.am.buffertree.api.IOrderedTupleIterator;
import org.apache.hyracks.storage.am.buffertree.api.ITupleIterator;
import org.apache.hyracks.storage.am.buffertree.impl.FrameOpStatus;

public interface ITreeBufferFrame {

    int getBufferTupleCount();
    int getBufferFreeSpace();
    int getBufferCapacity();

    void resetBufferSpaceParams();
    void resetBufferState();

    FrameOpStatus insertIntoBuffer(ITupleReference tuple) throws HyracksDataException;
    FrameOpStatus insertIntoBuffer(ITupleIterator iterator) throws HyracksDataException;

    IOrderedTupleIterator getBufferIterator() throws HyracksDataException;
    IBulkTupleInserter getBufferBulkInserter();

}
