package org.apache.hyracks.storage.am.buffertree.impl;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ICursorInitialState;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.IndexException;

import java.util.PriorityQueue;


public class RangeSearchCursor implements IIndexCursor {

    protected RangeSearchPredicate pred;
    protected PriorityQueue<IIndexCursor> priorityQueue;


    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws IndexException, HyracksDataException {

    }

    public boolean hasNext() throws HyracksDataException {
        return false;
    }

    public void next() throws HyracksDataException {

    }

    public void close() throws HyracksDataException {

    }

    public void reset() throws HyracksDataException {

    }

    public ITupleReference getTuple() {
        return null;
    }
}
