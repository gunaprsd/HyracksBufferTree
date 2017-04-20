package org.apache.hyracks.storage.am.buffertree.impl;

public enum BufferTreeIndexOperation {
    INSERT,
    DELETE,
    UPDATE,
    UPSERT,
    POINT_LOOKUP,
    RANGE_SEARCH
}
