package org.apache.hyracks.storage.am.buffertree.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.impl.FrameOpStatus;
import org.apache.hyracks.storage.am.buffertree.impl.PriorityQueueIterator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

import java.util.concurrent.atomic.AtomicBoolean;

public interface IPrimaryBufferBucket {

    ITupleReference getMatchingKeyTuple(ITupleReference searchKey) throws HyracksDataException;

    FrameOpStatus insertIntoBuffer(ITupleReference tuple) throws HyracksDataException;

    FrameOpStatus insertIntoBuffer(ITupleIterator iterator) throws HyracksDataException;

    void initialize(IBufferCache bufferCache, int memFileId, int memFileOffset);

    void open();

    void close();

    boolean isValid();

    boolean isEmptying();

    void markValid();

    void markInvalid();

    void setEmptyingStatus(boolean emptying);

    IOrderedTupleIterator getIterator();

    void addIterators(PriorityQueueIterator pqIterator);

    int getSize();

    void reset();

    void acquireReadLock();

    void acquireWriteLock();

    void releaseReadLock();

    void releaseWriteLock();
}
