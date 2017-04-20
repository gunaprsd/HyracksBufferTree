package org.apache.hyracks.storage.am.buffertree.impl;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.IOrderedTupleIterator;
import org.apache.hyracks.storage.am.buffertree.common.KeyValueTupleWriter;

import java.util.ArrayList;

public class ArrayListIterator implements IOrderedTupleIterator {

    protected ArrayList<? extends ITupleReference> list;
    protected int current;

    public ArrayListIterator(ArrayList<? extends ITupleReference> list, int current) {
        this.list = list;
        this.current = current;
    }

    public void seek(ITupleReference tuple) throws HyracksDataException {
        throw new AssertionError();
    }

    public void setEndLimit(ITupleReference tuple) throws HyracksDataException {
        throw new AssertionError();
    }

    public ITupleReference getCurrentLimit() throws HyracksDataException {
        throw new AssertionError();
    }

    public boolean hasNext() throws HyracksDataException {
        return current < list.size();
    }

    public void next() throws HyracksDataException {
        if(current < list.size()) {
            current++;
        }
    }

    public ITupleReference peek() throws HyracksDataException {
        return list.get(current);
    }

    public int getPeekOffset() {
        throw new AssertionError();
    }

    public void unsafeUpdate(ITupleReference tuple, int off) {
        throw new AssertionError();
    }

    public int estimateSize() throws HyracksDataException {
        int size = 0;
        for(int i = current; i < list.size(); i++) {
            size += KeyValueTupleWriter.INSTANCE.bytesRequired(list.get(i)) + 4;
        }
        return size;
    }

    public void reset() {
        list = null;
        current = 0;
    }

    public boolean done() {
        return current >= list.size();
    }
}
