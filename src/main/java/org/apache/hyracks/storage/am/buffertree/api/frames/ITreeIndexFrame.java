package org.apache.hyracks.storage.am.buffertree.api.frames;


import org.apache.hyracks.storage.am.buffertree.api.ISlottedTupleManager;

public interface ITreeIndexFrame extends ICachedPageFrame {
    
     void initBuffer(byte level);

     boolean isLeaf();
     boolean isNode();
     boolean isLeafNode();

     int getPageHeaderSize();

     int getPreviousSiblingId();
     int getNextSiblingId();
     long getPageLsn();
     boolean getSmFlag();
     byte getLevel();

     void setPreviousSiblingId(int pageId);
     void setNextSiblingId(int pageId);
     void setPageLsn(long pageLsn);
     void setSmFlag(boolean smFlag);
     void setLevel(byte level);

     ISlottedTupleManager getIndexTupleManager();

}
