package org.urbcomp.startdb.selfstar.query;

import java.util.ArrayList;
import java.util.List;

public class BTreeNode {
    private List<CompressedBlock> blocks;
    private List<BTreeNode> children;
    private boolean leaf;
    private int degree;

    public BTreeNode(int degree, boolean leaf){
        this.blocks = new ArrayList<>();
        this.children = new ArrayList<>();
        this.leaf = leaf;
        this.degree = degree;
    }

    public CompressedBlock findWithBlockIndex (int blockindex){
        int i = 0;
        while (i < blocks.size() && blockindex > blocks.get(i).getIBlock()){
            i++;
        }
        if (i < blocks.size() && blockindex == blocks.get(i).getIBlock()){
            return blocks.get(i);
        }
        if (leaf) {
            return null;
        }
        return children.get(i).findWithBlockIndex(blockindex);
    }

    public CompressedBlock findwithDataIndex (int dataindex){
        if (dataindex < blocks.get(0).getIData())
            return children.get(0).findwithDataIndex(dataindex);
        for (int i = 0; i < blocks.size(); i++){
            if(dataindex >= blocks.get(i).getIData() && dataindex < blocks.get(i).getIData() + blocks.get(i).getDataNumber() ) {
                return blocks.get(i);
            }
            else if (dataindex >= blocks.get(i).getIData() + blocks.get(i).getDataNumber() && dataindex < blocks.get(i+1).getIData() && i < blocks.size()-1){
                return children.get(i+1).findwithDataIndex(dataindex);
            }
            else if (dataindex >= blocks.get(i).getIData() + blocks.get(i).getDataNumber() && i == blocks.size()-1){
                    return children.get(i+1).findwithDataIndex(dataindex);
            }
        }
        return null;
    }



    public boolean isFull(){
        return blocks.size() == 2 * degree - 1;
    }

    public void splitChild (int i, BTreeNode child){
        BTreeNode newNode = new BTreeNode(child.degree, child.leaf);
        newNode.blocks.addAll(child.blocks.subList(degree, 2 * degree - 1));
        child.blocks.subList(degree, 2 * degree - 1).clear();
        if (!child.leaf) {
            newNode.children.addAll(child.children.subList(degree, 2 * degree));
            child.children.subList(degree, 2 * degree).clear();
        }
        children.add(i + 1, newNode);
        blocks.add(i, child.blocks.remove(degree - 1));
    }

    public void addChild(BTreeNode root){
        children.add(root);
    }

    public void insertNonFull(CompressedBlock block) {
        int i = blocks.size() - 1;
        if (leaf) {
            while (i >= 0 && block.getIBlock() < blocks.get(i).getIBlock()) {
                i--;
            }
            blocks.add(i + 1, block);
        }
        else {
            while (i >= 0 && block.getIBlock() < blocks.get(i).getIBlock()) {
                i--;
            }
            i++;
            if (children.get(i).isFull()) {
                splitChild(i, children.get(i));
                if (block.getIBlock() > blocks.get(i).getIBlock()) {
                    i++;
                }
            }
            children.get(i).insertNonFull(block);
        }
    }

}
