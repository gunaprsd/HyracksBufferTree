package org.apache.hyracks.storage.am.buffertree.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.frames.IPrimaryBufferFrame;
import org.apache.hyracks.storage.am.buffertree.impl.BufferTree;
import org.apache.hyracks.storage.am.buffertree.impl.FrameOpStatus;
import org.apache.hyracks.storage.am.buffertree.impl.PriorityQueueIterator;

import java.io.PrintStream;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public interface IPrimaryBufferManager {

    void initialize(BufferTree tree);

    void open();

    IOrderedTupleIterator getIterator();

    ITupleReference getMatchingKeyTuple(ITupleReference searchKey) throws HyracksDataException;

    void insert(ITupleIterator iterator) throws HyracksDataException;

    void insert(ITupleReference tuple) throws HyracksDataException;

    void close();
}
