package org.apache.hyracks.storage.am.buffertree.api.common;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

import java.nio.ByteBuffer;

public interface IReusableTupleReference extends ITupleReference {

    boolean isNull();

    void reset();

    void reset(ITupleReference tuple);

    void reset(ByteBuffer buffer, int offset);

    void reset(byte[] data, int offset, int length);

    void deepCopy(ITupleReference tuple);

}
