package org.urbcomp.startdb.selfstar.query;

public class BTree {
    private BTreeNode root;
    private int degree;
    private int TreeNodeNumber;

    public BTree(int degree){
        this.degree = degree;
        this.root = new BTreeNode(degree,true);
        this.TreeNodeNumber = 0;
    }

    public CompressedBlock findWithBlockIndex (int blockIndex){
        if (root == null) return  null;
        return root.findWithBlockIndex(blockIndex);
    }

    public CompressedBlock findWithDataIndex (int dataindex){
        if (root == null) return null;
        return root.findwithDataIndex(dataindex);
    }

    public void insert(CompressedBlock block) {
        this.TreeNodeNumber += 1;
        if (root.isFull()) {
            BTreeNode newRoot = new BTreeNode(degree, false);
            newRoot.addChild(root);
            newRoot.splitChild(0, root);
            root = newRoot;
        }
        root.insertNonFull(block);
    }

    public int getTreeNodeNumber(){return TreeNodeNumber;}

}
