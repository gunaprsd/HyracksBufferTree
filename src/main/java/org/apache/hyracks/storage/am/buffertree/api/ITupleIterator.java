package org.apache.hyracks.storage.am.buffertree.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

//TODO make the bufferIterator reuse the tuple variables instead of creating a new one every time.
public interface ITupleIterator {

    boolean hasNext() throws HyracksDataException;

    //changing it to void next() to reduce object allocations
    void next() throws HyracksDataException;

    ITupleReference peek() throws HyracksDataException;

    int getPeekOffset();

    void unsafeUpdate(ITupleReference tuple, int off);

    int estimateSize() throws HyracksDataException;

    void reset();

    boolean done();
}
