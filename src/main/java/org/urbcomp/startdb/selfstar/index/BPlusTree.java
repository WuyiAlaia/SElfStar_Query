package org.urbcomp.startdb.selfstar.index;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import org.urbcomp.startdb.selfstar.query.CompressedChunk;

public class BPlusTree<K extends Comparable<K>, V extends Serializable> {
    private BPlusTreeNode<K, V> root;
    private int degree;
    private RandomAccessFile storageFile;
    private int chunkSize;

    public BPlusTree(int degree, String storageFilePath, int chunkSize) throws IOException {
        this.root = new BPlusTreeNode<>(true);
        this.degree = degree;
        this.storageFile = new RandomAccessFile(storageFilePath, "rw");
        this.chunkSize = chunkSize;
    }

    public void insert(K key, V value) throws IOException {
        BPlusTreeNode<K, V> r = this.root;
        if (r.keys.size() == (2 * degree - 1)) {
            BPlusTreeNode<K, V> s = new BPlusTreeNode<>(false);
            this.root = s;
            s.children.add(r);
            splitChild(s, 0);
            insertNonFull(s, key, value);
        } else {
            insertNonFull(r, key, value);
        }
    }

    private void insertNonFull(BPlusTreeNode<K, V> x, K key, V value) throws IOException {
        int i = x.keys.size() - 1;
        if (x.isLeaf) {
            x.keys.add(null);
            x.values.add(null);
            while (i >= 0 && key.compareTo(x.keys.get(i)) < 0) {
                x.keys.set(i + 1, x.keys.get(i));
                x.values.set(i + 1, x.values.get(i));
                i--;
            }
            x.keys.set(i + 1, key);
            long offset = storeValue(value);
            x.values.set(i + 1, offset);
        } else {
            while (i >= 0 && key.compareTo(x.keys.get(i)) < 0) {
                i--;
            }
            i++;
            BPlusTreeNode<K, V> child = x.children.get(i);
            if (child.keys.size() == (2 * degree - 1)) {
                splitChild(x, i);
                if (key.compareTo(x.keys.get(i)) > 0) {
                    i++;
                }
            }
            insertNonFull(x.children.get(i), key, value);
        }
    }

    private void splitChild(BPlusTreeNode<K, V> x, int i) {
        BPlusTreeNode<K, V> y = x.children.get(i);
        BPlusTreeNode<K, V> z = new BPlusTreeNode<>(y.isLeaf);
        x.children.add(i + 1, z);
        x.keys.add(i, y.keys.get(degree - 1));

        for (int j = 0; j < degree - 1; j++) {
            z.keys.add(y.keys.remove(degree));
            if (y.isLeaf) {
                z.values.add(y.values.remove(degree));
            }
        }

        if (!y.isLeaf) {
            for (int j = 0; j < degree; j++) {
                z.children.add(y.children.remove(degree));
            }
        }
    }

    public V search(K key) throws IOException, ClassNotFoundException {
        return search(this.root, key);
    }

    private V search(BPlusTreeNode<K, V> x, K key) throws IOException, ClassNotFoundException {
        int i = 0;
        while (i < x.keys.size() && key.compareTo(x.keys.get(i)) > 0) {
            i++;
        }
        if (i < x.keys.size() && key.compareTo(x.keys.get(i)) == 0) {
            if (x.isLeaf) {
                return loadValue(x.values.get(i));
            } else {
                return search(x.children.get(i + 1), key);
            }
        } else if (x.isLeaf) {
            return null;
        } else {
            return search(x.children.get(i), key);
        }
    }

    private long storeValue(V value) throws IOException {
        long offset = storageFile.length();
        storageFile.seek(offset);
        byte[] bytes = ((CompressedChunk) value).toBytes();
        storageFile.write(bytes);
        return offset;
    }

    private V loadValue(long offset) throws IOException, ClassNotFoundException {
        byte[] bytes = new byte[chunkSize];
        storageFile.seek(offset);
        storageFile.read(bytes);
        return (V) CompressedChunk.fromBytes(bytes);
    }
}

class BPlusTreeNode<K extends Comparable<K>, V> {
    boolean isLeaf;
    List<K> keys;
    List<Long> values;  // Store file offsets for leaf node values
    List<BPlusTreeNode<K, V>> children;

    BPlusTreeNode(boolean isLeaf) {
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<>();
        this.values = new ArrayList<>();
        this.children = new ArrayList<>();
    }
}