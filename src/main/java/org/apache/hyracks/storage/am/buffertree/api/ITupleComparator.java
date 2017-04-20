package org.apache.hyracks.storage.am.buffertree.api;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface ITupleComparator {

    int compare(ITupleReference tupleA, ITupleReference tupleB);
}
