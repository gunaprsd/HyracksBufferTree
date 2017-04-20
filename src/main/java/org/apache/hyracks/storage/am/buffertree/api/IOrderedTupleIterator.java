package org.apache.hyracks.storage.am.buffertree.api;


import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface IOrderedTupleIterator extends  ITupleIterator {

    void seek(ITupleReference tuple) throws HyracksDataException;

    void setEndLimit(ITupleReference tuple) throws HyracksDataException;

    ITupleReference getCurrentLimit() throws HyracksDataException;

}
