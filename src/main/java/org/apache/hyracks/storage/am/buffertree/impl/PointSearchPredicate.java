package org.apache.hyracks.storage.am.buffertree.impl;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;

public class PointSearchPredicate implements ISearchPredicate {

    public ITupleReference searchKey;

    public PointSearchPredicate(ITupleReference searchKey) {
        this.searchKey = searchKey;
    }

    public void reset(ITupleReference searchKey) {
        this.searchKey = searchKey;
    }

    public MultiComparator getLowKeyComparator() {
        return null;
    }

    public MultiComparator getHighKeyComparator() {
        return null;
    }
}
