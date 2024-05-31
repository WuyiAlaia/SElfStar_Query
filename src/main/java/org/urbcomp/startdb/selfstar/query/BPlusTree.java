package org.urbcomp.startdb.selfstar.query;


public class BPlusTree {
    private BPlusTreeNode root;
    private int maxKeys;

    public BPlusTree(int maxKeys) {
        this.root = new BPlusTreeLeafNode();
        this.maxKeys = maxKeys;
    }

    public void insert(int key, String fileReference) {
        BPlusTreeNode newChild = insert(root, key, fileReference);
        if (newChild != null) {
            BPlusTreeInternalNode newRoot = new BPlusTreeInternalNode();
            newRoot.keys.add(root.keys.get(0));
            newRoot.keys.add(newChild.keys.get(0));
            newRoot.children.add(root);
            newRoot.children.add(newChild);
            root = newRoot;
        }
    }

    // 0,44,66
    private BPlusTreeNode insert(BPlusTreeNode node, int key, String fileReference) {
        if (node.isLeaf) {
            BPlusTreeLeafNode leaf = (BPlusTreeLeafNode) node;
            int pos = 0;
            while (pos < leaf.keys.size() && key > leaf.keys.get(pos)) {
                pos++;
            }
            leaf.keys.add(pos, key);
            leaf.fileReferences.add(pos, fileReference);
            if (leaf.keys.size() > maxKeys) {
                return splitLeafNode(leaf);
            } else {
                return null;
            }
        } else {
            BPlusTreeInternalNode internal = (BPlusTreeInternalNode) node;
            int pos = 0;
            while (pos < internal.keys.size() && key > internal.keys.get(pos)) {
                pos++;
            }
            BPlusTreeNode newChild = insert(internal.children.get(pos-1), key, fileReference);
            if (newChild != null) {
                internal.keys.add(pos, newChild.keys.get(0));
                internal.children.add(pos, newChild);
                if (internal.keys.size() > maxKeys) {
                    return splitInternalNode(internal);
                }
            }
            return null;
        }
    }

    private BPlusTreeNode splitLeafNode(BPlusTreeLeafNode leaf) {
        BPlusTreeLeafNode newLeaf = new BPlusTreeLeafNode();
        int mid = (maxKeys + 1) / 2;

        newLeaf.keys.addAll(leaf.keys.subList(mid, leaf.keys.size()));
        newLeaf.fileReferences.addAll(leaf.fileReferences.subList(mid, leaf.fileReferences.size()));

        leaf.keys.subList(mid, leaf.keys.size()).clear();
        leaf.fileReferences.subList(mid, leaf.fileReferences.size()).clear();

        newLeaf.next = leaf.next;
        leaf.next = newLeaf;

        return newLeaf;
    }

    private BPlusTreeNode splitInternalNode(BPlusTreeInternalNode internal) {
        BPlusTreeInternalNode newInternal = new BPlusTreeInternalNode();
        int mid = (maxKeys + 1) / 2;

        newInternal.keys.addAll(internal.keys.subList(mid, internal.keys.size()));
        newInternal.children.addAll(internal.children.subList(mid, internal.children.size()));

        internal.keys.subList(mid, internal.keys.size()).clear();
        internal.children.subList(mid, internal.children.size()).clear();

        return newInternal;
    }

    public BPlusTreeLeafNode searchLeafNode (int key){
        return searchLeafNode (root, key);
    }
    public BPlusTreeLeafNode searchLeafNode (BPlusTreeNode node, int key){
        if (node.isLeaf){
            BPlusTreeLeafNode leaf = (BPlusTreeLeafNode) node;
            return leaf;
        }
        else {
            BPlusTreeInternalNode internal = (BPlusTreeInternalNode) node;
            int pos = internal.keys.size() - 1;
            while (pos > 0 && key < internal.keys.get(pos)) {
                pos --;
            }
            return searchLeafNode(internal.children.get(pos), key);
        }
    }


    public String search (int key){
        return search(root, key);
    }
    private String search (BPlusTreeNode node,int key){
        if (node.isLeaf) {
            BPlusTreeLeafNode leaf = (BPlusTreeLeafNode) node;
            int pos = leaf.keys.size() -1;
            while (pos > 0 && key < leaf.keys.get(pos)) {
                pos--;
            }
            return leaf.fileReferences.get(pos);
        } else {
            BPlusTreeInternalNode internal = (BPlusTreeInternalNode) node;
            int pos = internal.keys.size() - 1;
            while (pos > 0 && key < internal.keys.get(pos)) {
                pos --;
            }
            return search(internal.children.get(pos), key);
        }
    }

    public BPlusTreeNode getRoot(){
        return root;
    }
    public Integer getMaxKey() {return maxKeys;}
}
