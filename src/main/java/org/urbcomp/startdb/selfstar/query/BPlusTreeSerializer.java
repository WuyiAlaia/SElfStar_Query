package org.urbcomp.startdb.selfstar.query;

import org.urbcomp.startdb.selfstar.query.BPlusTree;
import org.urbcomp.startdb.selfstar.query.BPlusTreeNode;
import org.urbcomp.startdb.selfstar.query.BPlusTreeLeafNode;
import org.urbcomp.startdb.selfstar.query.BPlusTreeInternalNode;

import java.io.*;
import java.util.*;

public class BPlusTreeSerializer {

    // 将B+树的索引结构序列化为文件
    public static void serialize(BPlusTree tree, String filePath) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(filePath))) {
            writer.println(tree.getMaxKey());
            serializeNode(tree.getRoot(), writer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void serializeNode(BPlusTreeNode node, PrintWriter writer) {
        if (node.getNodeIsLeaf()) {
            BPlusTreeLeafNode leaf = (BPlusTreeLeafNode) node;
            for (int i = 0; i < leaf.getKeys().size(); i++) {
                writer.println(leaf.getKeys().get(i) + "," + leaf.getFileReferences().get(i));
            }
        } else {
            BPlusTreeInternalNode internal = (BPlusTreeInternalNode) node;
            for (int i = 0; i < internal.getKeys().size(); i++) {
                serializeNode(internal.getChildren().get(i), writer);
                // writer.println(internal.getKeys().get(i));
            }
            // serializeNode(internal.getChildren().get(internal.getChildren().size() - 1), writer);
        }
    }

    // 从文件中读取B+树的索引结构并构建树
    public static BPlusTree deserialize(String filePath) {
        BPlusTree tree = null;
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line = reader.readLine(); // Read the first line for maxKeys
            if (line != null) {
                int maxKeys = Integer.parseInt(line.trim());
                tree = new BPlusTree(maxKeys);
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",");
                    int key = Integer.parseInt(parts[0].trim());
                    String fileReference = parts[1].trim();
                    tree.insert(key, fileReference);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NumberFormatException e) {
            System.err.println("Invalid format for maxKeys in the first line of the file.");
            e.printStackTrace();
        }
        return tree;
    }
}
