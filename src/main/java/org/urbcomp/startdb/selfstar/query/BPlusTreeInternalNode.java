package org.urbcomp.startdb.selfstar.query;

import java.util.ArrayList;
import java.util.List;

public class BPlusTreeInternalNode extends BPlusTreeNode {
    List<BPlusTreeNode> children;

    BPlusTreeInternalNode() {
        super(false);
        this.children = new ArrayList<>();
    }

    @Override
    public List<Integer> getKeys() {
        return super.getKeys();
    }
    @Override
    public boolean getNodeIsLeaf(){ return super.getNodeIsLeaf();}
    public List<BPlusTreeNode> getChildren() { return children;}
}
