package org.apache.hyracks.storage.am.buffertree.common;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.ITupleComparator;

import java.util.Comparator;

/**
 * Created by t-guje on 10/24/2015.
 */
public class KeyValueComparator implements ITupleComparator, Comparator<ITupleReference> {

    public static final KeyValueComparator INSTANCE = new KeyValueComparator();

    public int compare(ITupleReference left, ITupleReference right) {
        byte leftType = ((KeyValueTuple)left).getType();
        byte rightType = ((KeyValueTuple)right).getType();

        if(leftType == KeyValueTuple.HIGHEST_KEY) {
            if(rightType == KeyValueTuple.HIGHEST_KEY) {
                return 0;
            } else {
                return 1;
            }
        } else if(leftType == KeyValueTuple.LOWEST_KEY) {
            if(rightType == KeyValueTuple.LOWEST_KEY) {
                return 0;
            } else {
                return -1;
            }
        } else if(rightType == KeyValueTuple.LOWEST_KEY) {
            return 1;
        } else if(rightType == KeyValueTuple.HIGHEST_KEY) {
            return -1;
        } else {
            return compareTo(left.getFieldData(1), left.getFieldStart(1), left.getFieldLength(1),
                    right.getFieldData(1), right.getFieldStart(1), right.getFieldLength(1));
        }
    }

    //Code copied from apache commons
    private int compareTo(byte[] buffer1, int offset1, int length1,
                                 byte[] buffer2, int offset2, int length2) {
        // Short circuit equal case
        if (buffer1 == buffer2 &&
                offset1 == offset2 &&
                length1 == length2) {
            return 0;
        }
        int end1 = offset1 + length1;
        int end2 = offset2 + length2;
        for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
            int a = (buffer1[i] & 0xff);
            int b = (buffer2[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return length1 - length2;
    }
}
