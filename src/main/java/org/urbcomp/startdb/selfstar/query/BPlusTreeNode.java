package org.urbcomp.startdb.selfstar.query;

import java.util.ArrayList;
import java.util.List;

abstract public class BPlusTreeNode {
    boolean isLeaf;
    List<Integer> keys;

    BPlusTreeNode(boolean isLeaf) {
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<>();
    }

    List<Integer> getKeys(){
        return keys;
    }
    public boolean getNodeIsLeaf(){ return isLeaf;}
}
