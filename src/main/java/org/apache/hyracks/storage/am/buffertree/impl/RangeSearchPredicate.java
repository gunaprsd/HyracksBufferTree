package org.apache.hyracks.storage.am.buffertree.impl;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;

/**
 * Created by gunaprsd on 29/10/15.
 */
public class RangeSearchPredicate implements ISearchPredicate {

    public ITupleReference lowKey;
    public ITupleReference highKey;
    public boolean lowKeyInclusive;
    public boolean highKeyInclusive;

    public RangeSearchPredicate(ITupleReference lowKey, ITupleReference highKey, boolean lowKeyInclusive, boolean highKeyInclusive) {
        this.lowKey = lowKey;
        this.highKey = highKey;
        this.lowKeyInclusive = lowKeyInclusive;
        this.highKeyInclusive = highKeyInclusive;
    }

    public void reset(ITupleReference lowKey, ITupleReference highKey, boolean lowKeyInclusive, boolean highKeyInclusive) {
        this.lowKey = lowKey;
        this.highKey = highKey;
        this.lowKeyInclusive = lowKeyInclusive;
        this.highKeyInclusive = highKeyInclusive;
    }

    public MultiComparator getLowKeyComparator() {
        return null;
    }

    public MultiComparator getHighKeyComparator() {
        return null;
    }
}
