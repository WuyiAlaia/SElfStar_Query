package org.urbcomp.startdb.selfstar.query;

import java.util.ArrayList;
import java.util.List;

public class MetaIndex {
    private int k; // 每个节点的子节点数量
    private Info[][] nodeTree;

    static class Info {
        double min, max, sum;

        public Info(double min, double max, double sum) {
            this.min = min;
            this.max = max;
            this.sum = sum;
        }
    }

    public MetaIndex(int k) {
        this.k = k;
    }

    public void createNodeTree(List<MetaData> metaDataList) {
        int currentLength = metaDataList.size();
        int level = (int) Math.ceil(Math.log(currentLength) / Math.log(k)) + 1;
        nodeTree = new Info[level][];
        nodeTree[level - 1] = new Info[currentLength];

        for (int i = 0; i < currentLength; i++) {
            nodeTree[level - 1][i] = new Info(metaDataList.get(i).getMinValue(), metaDataList.get(i).getMaxValue(), metaDataList.get(i).getSum());
        }

        for (int i = level - 2; i >= 0; i--) {
            currentLength = (currentLength + k - 1) / k;
            nodeTree[i] = new Info[currentLength];
            for (int j = 0; j < currentLength; j++) {
                double min = Double.MAX_VALUE;
                double max = Double.MIN_VALUE;
                double sum = 0;
                for (int m = 0; m < k; m++) {
                    int index = k * j + m;
                    if (index < nodeTree[i + 1].length && nodeTree[i + 1][index] != null) {
                        min = Math.min(min, nodeTree[i + 1][index].min);
                        max = Math.max(max, nodeTree[i + 1][index].max);
                        sum += nodeTree[i + 1][index].sum;
                    }
                }
                nodeTree[i][j] = new Info(min, max, sum);
            }
        }
    }

    public static void main(String[] args) {
        // 创建测试数据
        List<MetaData> metaDataList = new ArrayList<>();
        metaDataList.add(createMetaData(0, 1.0, 5.0, 10.0, 1));
        metaDataList.add(createMetaData(1, 2.0, 6.0, 12.0, 1));
        metaDataList.add(createMetaData(2, 0.5, 4.5, 8.0, 1));
        metaDataList.add(createMetaData(3, 3.0, 7.0, 14.0, 1));
        metaDataList.add(createMetaData(4, 6.0, 7.0, 14.0, 1));
        metaDataList.add(createMetaData(5, 5.0, 7.0, 14.0, 1));

        // 创建MetaIndex
        int k = 3; // k 叉树
        MetaIndex metaIndex = new MetaIndex(k);

        // 构建索引树
        metaIndex.createNodeTree(metaDataList);

        // 输出构建好的索引树，用于验证
        printMetaIndex(metaIndex);
    }

    private static MetaData createMetaData(int index, double min, double max, double sum, int dataNum) {
        MetaData metaData = new MetaData(index);
        metaData.setMinValue(min);
        metaData.setMaxValue(max);
        metaData.setSum(sum);
        metaData.setDataNum(dataNum);
        return metaData;
    }

    private static void printMetaIndex(MetaIndex metaIndex) {
        for (int i = 0; i < metaIndex.nodeTree.length; i++) {
            System.out.println("Level " + i + ":");
            for (int j = 0; j < metaIndex.nodeTree[i].length; j++) {
                Info info = metaIndex.nodeTree[i][j];
                if (info != null) {
                    System.out.println("  Node " + j + " - Min: " + info.min + ", Max: " + info.max + ", Sum: " + info.sum);
                }
            }
        }
    }
}
