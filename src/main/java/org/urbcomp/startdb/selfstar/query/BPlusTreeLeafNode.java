package org.urbcomp.startdb.selfstar.query;

import java.util.ArrayList;
import java.util.List;

public class BPlusTreeLeafNode extends BPlusTreeNode {
    List<String> fileReferences;
    BPlusTreeLeafNode next;

    BPlusTreeLeafNode() {
        super(true);
        this.fileReferences = new ArrayList<>();
        this.next = null;
    }

    @Override
    public List<Integer> getKeys() {
        return super.getKeys();
    }
    @Override
    public boolean getNodeIsLeaf(){ return super.getNodeIsLeaf();}
    public List<String> getFileReferences() { return fileReferences;}

    public BPlusTreeLeafNode getNext() { return next;}
}
