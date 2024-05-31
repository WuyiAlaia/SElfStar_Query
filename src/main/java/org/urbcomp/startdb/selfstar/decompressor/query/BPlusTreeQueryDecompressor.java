package org.urbcomp.startdb.selfstar.decompressor.query;

import org.urbcomp.startdb.selfstar.query.CompressedChunk;

import java.util.Collections;
import java.util.List;

//
//import org.urbcomp.startdb.selfstar.decompressor.IDecompressor;
//import org.urbcomp.startdb.selfstar.query.*;
//import org.urbcomp.startdb.selfstar.utils.BlockReader;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.ObjectInputStream;
//import java.util.ArrayList;
//import java.util.List;
//
//
public class BPlusTreeQueryDecompressor implements IQueryDecompressor{
    @Override
    public double decompress(CompressedChunk compressedChunk, int index) {
        return 0;
    }

    @Override
    public List<Double> decompress(CompressedChunk compressedChunk, int startIndex, int endIndex) {
        return Collections.emptyList();
    }

    @Override
    public List<Double> decompress(CompressedChunk compressedChunk) {
        return Collections.emptyList();
    }
//    private static final String folderPath_Bytes_Tree = "D:/bytes/TreeBytes/";
//    private final IDecompressor decompressor;
//    private final BPlusTree blockFilesTree;
//    private final CompressedBlock block = new CompressedBlock();
//    public BPlusTreeQueryDecompressor(IDecompressor decompressor, String datasetFileName) {
//        this.decompressor = decompressor;
//        // this.blockFilesTree = blockFilesTree;
//        this.blockFilesTree =  BPlusTreeSerializer.deserialize(folderPath_Bytes_Tree + datasetFileName + "/" + "blockFiles.txt");
//    }
//
//
//    public List<Double> decompress(int valuesNumber) {
//
//        List<Double> values = new ArrayList<>(valuesNumber);
//
//        for (int i = 0; i < valuesNumber; i++) {
//            values.add(decompressor.nextValue());
//        }
//        return values;
//    }
//    public void setBytes(byte[] bs){
//        decompressor.setBytes(bs);
//    }
//
//    public Double randomQuery(int index){
//        String filePath = blockFilesTree.search(index);
//        File file = new File(filePath);
//        decompressor.refresh();
//        block.readFromFile(file);
//        decompressor.setBytes(block.getData());
//
//        String fileName = file.getName();
//        int iData = Integer.parseInt(fileName.substring(0, fileName.lastIndexOf('.')));
//        int dataIndexInBlock = index - iData ;
//        List<Double> floatings = decompress(dataIndexInBlock+1);
//        return floatings.get(dataIndexInBlock);
//    }
//
//
//    public List<Integer> rangeQuery(double f){
//        List<Integer> result = new ArrayList<>();
//
//        BPlusTreeLeafNode node = blockFilesTree.searchLeafNode(0);
//        int lastDataIndex = block.readIDataFromFile(new File(node.getFileReferences().get(0)));
//        while (node != null){
//            for (int i = 0; i < node.getFileReferences().size(); i++){
//
//                String blockFilePath = node.getFileReferences().get(i);
//
//                File file = new File(blockFilePath);
//                double maxInBlock = block.readMaxValueFromFile(file);
//                if (f > maxInBlock){
//                    break;
//                }
//                double minInBlock = block.readMinValueFromFile(file);
//                if (f < minInBlock){
//                    break;
//                }
//
//                int indexOfFirstData = Integer.parseInt(file.getName().substring(0, file.getName().lastIndexOf('.')));
//                int dataNumberInBlock;
//                if ( i != node.getFileReferences().size()-1){
//                    String nextBlockFilePath = node.getFileReferences().get(i+1);
//                    File nextFile = new File(nextBlockFilePath);
//                    int idataOfNextBlock = Integer.parseInt(nextFile.getName().substring(0, nextFile.getName().lastIndexOf('.')));
//                    dataNumberInBlock = idataOfNextBlock - indexOfFirstData;
//                }
//                else if (node.getNext() != null){
//                    String nextBlockFilePath = node.getNext().getFileReferences().get(0);
//                    File nextFile = new File(nextBlockFilePath);
//                    int idataOfNextBlock = Integer.parseInt(nextFile.getName().substring(0, nextFile.getName().lastIndexOf('.')));
//                    dataNumberInBlock = idataOfNextBlock - indexOfFirstData;
//                }
//                else {      // the last block/file
//                    dataNumberInBlock = lastDataIndex - indexOfFirstData;
//                }
//                decompressor.refresh();
//                block.readFromFile(file);
//                decompressor.setBytes(block.getData());
//                List<Double> floatings = decompress(dataNumberInBlock);
//                for (int j=0; j < floatings.size(); j++){
//                    if (Double.toString(f).equals(Double.toString(floatings.get(j)))){
//                        result.add(j + indexOfFirstData);
//                    }
//                }
//            }
//            node = node.getNext();
//        }
//
//        return result;
//    }
//
//    public Double maxQuery(int startIndex, int endIndex){
//        if (startIndex == endIndex){
//            return randomQuery(startIndex);
//        }
//        Double max;
//        if(startIndex > endIndex){
//            int tmp = startIndex;
//            startIndex = endIndex;
//            endIndex = tmp;
//        }
//
//        BPlusTreeLeafNode startNode = blockFilesTree.searchLeafNode(startIndex);
//        List<Integer> startKeys = startNode.getKeys();
//        int startPos = startKeys.size() -1;
//        while (startPos > 0 && startIndex < startKeys.get(startPos)) {
//            startPos--;
//        }
//        String startFilePath = startNode.getFileReferences().get(startPos);
//        BPlusTreeLeafNode endNode = blockFilesTree.searchLeafNode(endIndex);
//        List<Integer> endKeys = endNode.getKeys();
//        int endPos = endKeys.size() -1;
//        while (endPos > 0 && endIndex < endKeys.get(endPos)) {
//            endPos--;
//        }
//        String endFilePath = endNode.getFileReferences().get(endPos);
//
//        if(startFilePath.equals(endFilePath)){
//            max = maxValueFormiToj(startIndex,endIndex,startFilePath);
//        }
//        else{
//            max = maxValueFromiToEnd(startIndex ,startNode, startPos);
//            Double max2 = maxValueForm0Toi(endIndex ,endFilePath);
//            if (max < max2){
//                max = max2;
//            }
//
//            boolean ifNeighbor = false;
//            if (startNode.getKeys().get(0) == endNode.getKeys().get(0)){
//                if (startPos + 1 == endPos){
//                    ifNeighbor = true;
//                }
//            } else {
//                if (startPos == startKeys.size()-1  && endPos == 0 && startNode.getNext().getKeys().get(0) == endNode.getKeys().get(0)){
//                    ifNeighbor = true;
//                }
//            }
//
//            if (!ifNeighbor){
//                for (int i = startPos + 1; i < startNode.getKeys().size();i++){
//                    String blockFilePath = startNode.getFileReferences().get(i);
//                    double maxInBlock = block.readMaxValueFromFile(new File(blockFilePath));
//                    if (max < maxInBlock){
//                        max = maxInBlock;
//                    }
//                }
//                BPlusTreeLeafNode node = startNode.getNext();
//                while (node != null){
//                    if (node.getKeys().get(0) != endNode.getKeys().get(0)){
//                        for (String blockFilePath:node.getFileReferences()){
//                            double maxInBlock = block.readMaxValueFromFile(new File(blockFilePath));
//                            if (max < maxInBlock){
//                                max = maxInBlock;
//                            }
//                        }
//                    } else {
//                        for (int i = 0; i < endPos;i++){
//                            String blockFilePath = node.getFileReferences().get(i);
//                            double maxInBlock = block.readMaxValueFromFile(new File(blockFilePath));
//                            if (max < maxInBlock){
//                                max = maxInBlock;
//                            }
//
//                        }
//                        break;
//                    }
//                    node = node.getNext();
//                }
//            }
//        }
//        return max;
//    }
//    public Double minQuery(int startIndex, int endIndex){
//        if (startIndex == endIndex){
//            return randomQuery(startIndex);
//        }
//        Double min;
//        if(startIndex > endIndex){
//            int tmp = startIndex;
//            startIndex = endIndex;
//            endIndex = tmp;
//        }
//
//        BPlusTreeLeafNode startNode = blockFilesTree.searchLeafNode(startIndex);
//        List<Integer> startKeys = startNode.getKeys();
//        int startPos = startKeys.size() -1;
//        while (startPos > 0 && startIndex < startKeys.get(startPos)) {
//            startPos--;
//        }
//        String startFilePath = startNode.getFileReferences().get(startPos);
//        BPlusTreeLeafNode endNode = blockFilesTree.searchLeafNode(endIndex);
//        List<Integer> endKeys = endNode.getKeys();
//        int endPos = endKeys.size() -1;
//        while (endPos > 0 && endIndex < endKeys.get(endPos)) {
//            endPos--;
//        }
//        String endFilePath = endNode.getFileReferences().get(endPos);
//
//        if(startFilePath.equals(endFilePath)){
//            min = minValueFormiToj(startIndex,endIndex,startFilePath);
//        }
//        else{
//            min = minValueFromiToEnd(startIndex ,startNode, startPos);
//            Double min2 = minValueForm0Toi(endIndex ,endFilePath);
//            if (min > min2){
//                min = min2;
//            }
//
//            boolean ifNeighbor = false;
//            if (startNode.getKeys().get(0) == endNode.getKeys().get(0)){
//                if (startPos + 1 == endPos){
//                    ifNeighbor = true;
//                }
//            } else {
//                if (startPos == startKeys.size()-1  && endPos == 0 && startNode.getNext().getKeys().get(0) == endNode.getKeys().get(0)){
//                    ifNeighbor = true;
//                }
//            }
//
//            if(!ifNeighbor){
//                for (int i = startPos + 1; i < startNode.getKeys().size();i++){
//                    String blockFilePath = startNode.getFileReferences().get(i);
//                    double minInBlock = block.readMinValueFromFile(new File(blockFilePath));
//                    if (min > minInBlock){
//                        min = minInBlock;
//                    }
//                }
//                BPlusTreeLeafNode node = startNode.getNext();
//                while (node != null){
//                    if (node.getKeys().get(0) != endNode.getKeys().get(0)){
//                        for (String blockFilePath:node.getFileReferences()){
//                            double minInBlock = block.readMinValueFromFile(new File(blockFilePath));
//                            if (min > minInBlock){
//                                min = minInBlock;
//                            }
//                        }
//                    } else {
//                        for (int i = 0; i < endPos;i++){
//                            String blockFilePath = node.getFileReferences().get(i);
//                            double minInBlock = block.readMinValueFromFile(new File(blockFilePath));
//                            if (min > minInBlock){
//                                min = minInBlock;
//                            }
//
//                        }
//                        break;
//                    }
//                    node = node.getNext();
//                }
//            }
//            }
//
//        return min;
//    }
//
//    public Double maxValueFromiToEnd(int fromIndex, BPlusTreeLeafNode leafNode, int pos){
//        String blockFilePath = leafNode.getFileReferences().get(pos);
//        decompressor.refresh();
//        block.readFromFile(new File(blockFilePath));
//        decompressor.setBytes(block.getData());
//        String blockFileName = new File(blockFilePath).getName();
//        int iData =Integer.parseInt(blockFileName.substring(0, blockFileName.lastIndexOf('.')));
//        fromIndex -= iData;
//        Double max = Double.MIN_VALUE;
//        Double value;
//        for (int i = 0; i < fromIndex; i ++){
//            decompressor.nextValue();
//        }
//        String nextFilePath;
//        if (pos == leafNode.getKeys().size()-1){
//            nextFilePath = leafNode.getNext().getFileReferences().get(0);
//        }
//        else{
//            nextFilePath = leafNode.getFileReferences().get(pos+1);
//        }
//        String currentFilePath = leafNode.getFileReferences().get(pos);
//
//        String nextFileName = (new File(nextFilePath)).getName();
//        String currentFileName = (new File(currentFilePath)).getName();
////        int lastSeparatorIndexInNextFile = nextFilePath.lastIndexOf("/"); // Unix/Linux
////        if (lastSeparatorIndexInNextFile == -1) {
////            lastSeparatorIndexInNextFile = nextFilePath.lastIndexOf("\\"); // Windows
////        }
////        String nextFileName = null;
////        if (lastSeparatorIndexInNextFile != -1) {
////            nextFileName = nextFilePath.substring(lastSeparatorIndexInNextFile + 1);
////        } else {
////            nextFileName = nextFilePath;
////        }
////        int lastSeparatorIndexInCurrentFile = currentFilePath.lastIndexOf("/");
////        if (lastSeparatorIndexInCurrentFile == -1){
////            lastSeparatorIndexInCurrentFile = currentFilePath.lastIndexOf("\\");
////        }
////        String currentFileName = null;
////        if (lastSeparatorIndexInCurrentFile != -1){
////            currentFileName = currentFilePath.substring(lastSeparatorIndexInCurrentFile + 1);
////        } else {
////            currentFileName = currentFilePath;
////        }
//
//        int nextBlockIData = Integer.parseInt(nextFileName.substring(0, nextFileName.lastIndexOf('.')));
//        int currentBlockIData = Integer.parseInt(currentFileName.substring(0, currentFileName.lastIndexOf('.')));
//        int valuesNumber = nextBlockIData - currentBlockIData;
//        for (int i = fromIndex; i < valuesNumber; i++){
//            value = decompressor.nextValue();
//            if (value > max){
//                max = value;
//            }
//        }
//        return max;
//
//    }
//    public Double maxValueForm0Toi(int toIndex,String blockFilePath){
//        decompressor.refresh();
//        block.readFromFile(new File(blockFilePath));
//        decompressor.setBytes(block.getData());
//        toIndex -= block.getIData();
//        Double max = Double.MIN_VALUE;
//        for (int i = 0; i <= toIndex; i++){
//            Double value = decompressor.nextValue();
//            if (value > max){
//                max = value;
//            }
//        }
//        return max;
//    }
//    public Double maxValueFormiToj (int startIndex,int endIndex, String blockFilePath){
//        decompressor.refresh();
//        block.readFromFile(new File(blockFilePath));
//        decompressor.setBytes(block.getData());
//        Double max = Double.MIN_VALUE;
//        Double value;
//        startIndex -= block.getIData();
//        endIndex -= block.getIData();
//        for (int i = 0; i < startIndex; i++){
//            decompressor.nextValue();
//        }
//        for (int i = startIndex; i <= endIndex; i++){
//            value = decompressor.nextValue();
//            if (value > max){
//                max = value;
//            }
//        }
//        return max;
//    }
//    public Double minValueFromiToEnd(int fromIndex, BPlusTreeLeafNode leafNode, int pos){
//            String blockFilePath = leafNode.getFileReferences().get(pos);
//            decompressor.refresh();
//            block.readFromFile(new File(blockFilePath));
//            decompressor.setBytes(block.getData());
//            String blockFileName = new File(blockFilePath).getName();
//            int iData =Integer.parseInt(blockFileName.substring(0, blockFileName.lastIndexOf('.')));
//            fromIndex -= iData;
//            Double min = Double.MAX_VALUE;
//            Double value;
//            for (int i = 0; i < fromIndex; i ++){
//                decompressor.nextValue();
//            }
//            String nextFilePath;
//            if (pos == leafNode.getKeys().size()-1){
//                nextFilePath = leafNode.getNext().getFileReferences().get(0);
//            }
//            else{
//                nextFilePath = leafNode.getFileReferences().get(pos+1);
//            }
//            String currentFilePath = leafNode.getFileReferences().get(pos);
//
//            String nextFileName = (new File(nextFilePath)).getName();
//            String currentFileName = (new File(currentFilePath)).getName();
////        int lastSeparatorIndexInNextFile = nextFilePath.lastIndexOf("/"); // Unix/Linux
////        if (lastSeparatorIndexInNextFile == -1) {
////            lastSeparatorIndexInNextFile = nextFilePath.lastIndexOf("\\"); // Windows
////        }
////        String nextFileName = null;
////        if (lastSeparatorIndexInNextFile != -1) {
////            nextFileName = nextFilePath.substring(lastSeparatorIndexInNextFile + 1);
////        } else {
////            nextFileName = nextFilePath;
////        }
////        int lastSeparatorIndexInCurrentFile = currentFilePath.lastIndexOf("/");
////        if (lastSeparatorIndexInCurrentFile == -1){
////            lastSeparatorIndexInCurrentFile = currentFilePath.lastIndexOf("\\");
////        }
////        String currentFileName = null;
////        if (lastSeparatorIndexInCurrentFile != -1){
////            currentFileName = currentFilePath.substring(lastSeparatorIndexInCurrentFile + 1);
////        } else {
////            currentFileName = currentFilePath;
////        }
//
//            int nextBlockIData = Integer.parseInt(nextFileName.substring(0, nextFileName.lastIndexOf('.')));
//            int currentBlockIData = Integer.parseInt(currentFileName.substring(0, currentFileName.lastIndexOf('.')));
//            int valuesNumber = nextBlockIData - currentBlockIData;
//            for (int i = fromIndex; i < valuesNumber; i++){
//                value = decompressor.nextValue();
//                if (value < min){
//                    min = value;
//                }
//            }
//            return min;
//    }
//
//    public Double minValueForm0Toi(int toIndex,String blockFilePath){
//        decompressor.refresh();
//        block.readFromFile(new File(blockFilePath));
//        decompressor.setBytes(block.getData());
//        toIndex -= block.getIData();
//        Double min = Double.MAX_VALUE;
//        for (int i = 0; i <= toIndex; i++){
//            Double value = decompressor.nextValue();
//            if (value < min ){
//                min = value;
//            }
//        }
//        return min;
//    }
//    public Double minValueFormiToj (int startIndex,int endIndex, String blockFilePath){
//        decompressor.refresh();
//        block.readFromFile(new File(blockFilePath));
//        decompressor.setBytes(block.getData());
//        Double min = Double.MAX_VALUE;
//        Double value;
//        startIndex -= block.getIData();
//        endIndex -= block.getIData();
//        for (int i = 0; i < startIndex; i++){
//            decompressor.nextValue();
//        }
//        for (int i = startIndex; i <= endIndex; i++){
//            value = decompressor.nextValue();
//            if (value < min){
//                min = value;
//            }
//        }
//        return min;
//    }
//
//
//    public void refresh(){
//        decompressor.refresh();
//    }
}
