package org.apache.hyracks.storage.am.buffertree.api;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.impl.FrameOpStatus;

public interface IBulkTupleInserter {

    void init();

    int getBytesRequiredToWriteTuple(ITupleReference tuple);

    FrameOpStatus insert(ITupleReference tuple);

    void end();

    void reset();
}
