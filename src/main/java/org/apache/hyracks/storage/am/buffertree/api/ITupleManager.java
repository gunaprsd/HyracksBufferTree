package org.apache.hyracks.storage.am.buffertree.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.impl.FrameOpStatus;

public interface ITupleManager {

    void initParams();

    int getStartOffset();

    int getHeaderSize();

    int getEndOffset();

    int getTupleCount();

    int getFreeSpace();

    void resetParams();

    void resetState();

    void resetSizeOffsets(int startOffset, int endOffset);

    int getBytesRequiredToWriteTuple(ITupleReference tuple);

    FrameOpStatus insertTuple(ITupleReference tuple) throws HyracksDataException;

    ITupleReference getMatchingKeyTuple(ITupleReference searchTuple) throws HyracksDataException;

    ITupleIterator getIterator();

    IOrderedTupleIterator getOrderedIterator() throws HyracksDataException;

    IBulkTupleInserter getBulkInserter();

    ITupleWriter getWriter();

    ITupleComparator getComparator();

}
