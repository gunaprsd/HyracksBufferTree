package org.apache.hyracks.storage.am.buffertree.api;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleReference;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleWithBloomFilter;
import org.apache.hyracks.storage.am.buffertree.api.common.IReusableTupleReference;

import java.nio.ByteBuffer;

public interface ITupleWriter {
    
     int writeTuple(ITupleReference tuple, ByteBuffer targetBuf, int targetOff);

     int writeTuple(ITupleReference tuple, byte[] targetBuf, int targetOff);

     int bytesRequired(ITupleReference tuple);

     int writeTupleFields(ITupleReference tuple, int startField, int numFields, byte[] targetBuf, int targetOff);

     int bytesRequired(ITupleReference tuple, int startField, int numFields);

     IReusableTupleReference createNewTuple();

     IReusableTupleReference createNewTuple(ITupleReference tuple);

     IIndexTupleReference createNewIndexTuple();

     IIndexTupleReference createNewIndexTuple(IIndexTupleReference tuple);

     IIndexTupleWithBloomFilter createNewIndexTupleWithBF();

}
