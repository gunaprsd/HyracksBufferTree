package org.apache.hyracks.storage.am.buffertree.impl;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.*;
import org.apache.hyracks.storage.am.buffertree.api.common.IReusableTupleReference;

public class MergeTupleIterator implements IOrderedTupleIterator {

    IOrderedTupleIterator iterator1;
    IOrderedTupleIterator iterator2;
    IOrderedTupleIterator currentIterator;
    ITupleWriter writer;
    ITupleComparator comparator;

    public MergeTupleIterator(IOrderedTupleIterator iter1, IOrderedTupleIterator iter2,
                              ITupleComparator comparator, ITupleWriter writer) throws HyracksDataException {
        iterator1 = iter1;
        iterator2 = iter2;
        this.comparator =comparator;
        this.writer = writer;
        this.currentIterator = null;
        select();
    }

    private void select() throws HyracksDataException {
        if(iterator1.hasNext() && iterator2.hasNext()) {
            int cmp = comparator.compare(iterator1.peek(), iterator2.peek());
            if(cmp < 0) {
                currentIterator = iterator1;
            } else {
                currentIterator = iterator2;
            }
        } else if(iterator1.hasNext()) {
            currentIterator = iterator1;
        } else if(iterator2.hasNext()){
            currentIterator = iterator2;
        }
    }
    public boolean hasNext() throws HyracksDataException {
        return currentIterator.hasNext();
    }

    public void next() throws HyracksDataException {
        if(!done()) {
            currentIterator.next();
            select();
        }
    }

    public ITupleReference peek() throws HyracksDataException {
        return currentIterator.peek();
    }

    public int getPeekOffset() {
        if (true) throw new AssertionError();
        return 0;
    }

    public void unsafeUpdate(ITupleReference tuple, int off) {
        if (true) throw new AssertionError();
    }

    public int estimateSize() throws HyracksDataException {
        return iterator1.estimateSize() + iterator2.estimateSize();
    }

    public void reset() {
        //never call this on merged bufferIterator
        if (!true) throw new AssertionError();
    }

    public boolean done() {
        return iterator1.done() && iterator2.done();
    }

    public void reset(IOrderedTupleIterator iter1, IOrderedTupleIterator iter2) throws HyracksDataException {
        iterator1 = iter1;
        iterator2 = iter2;
        select();
    }

    public void seek(ITupleReference tuple) throws HyracksDataException {
        iterator1.seek(tuple);
        iterator2.seek(tuple);
        select();
    }

    public void setEndLimit(ITupleReference tuple) throws HyracksDataException {
        iterator1.setEndLimit(tuple);
        iterator2.setEndLimit(tuple);
    }

    public ITupleReference getCurrentLimit() throws HyracksDataException {
        return iterator1.getCurrentLimit();
    }

}
