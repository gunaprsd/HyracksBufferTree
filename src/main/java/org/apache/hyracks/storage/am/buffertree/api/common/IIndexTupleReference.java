package org.apache.hyracks.storage.am.buffertree.api.common;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface IIndexTupleReference extends IReusableTupleReference {

    void setKey(ITupleReference key);

    int getPageId();

    void setPageId(int pageId);

    void reset(IIndexTupleReference indexTuple);
}
