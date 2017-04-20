package org.apache.hyracks.storage.am.buffertree.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.impl.FindTupleMode;

public interface ISlottedTupleManager extends ITupleManager {

    int getSlotSize();

    int getTupleOffsetByIndex(int index);

    int findTupleIndex(ITupleReference searchTuple, FindTupleMode mode) throws HyracksDataException;
}
