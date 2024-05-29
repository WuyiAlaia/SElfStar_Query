package org.urbcomp.startdb.selfstar.decompressor.query;

import org.urbcomp.startdb.selfstar.decompressor.IDecompressor;
import org.urbcomp.startdb.selfstar.query.CompressedBlock;
import org.urbcomp.startdb.selfstar.query.CompressedChunk;
import java.util.ArrayList;
import java.util.List;


public class QueryDecompressor implements IQueryDecompressor {
    private final IDecompressor decompressor;
    private final CompressedBlock block = new CompressedBlock();
    private List<Double> minValuesInBlocks = null;
    private List<Double> maxValuesInBlocks = null;
    private CompressedChunk compressedChunk = null;

    public QueryDecompressor(IDecompressor decompressor) {
        this.decompressor = decompressor;
        // this.blockFiles = blockFiles;
    }
    @Override
    public double decompress(CompressedChunk compressedChunk, int index) {
        double value = Double.NaN;
        decompressor.refresh();
        decompressor.setBytes(compressedChunk.getCompressedBytes());
        while (index > 0) {
            value = decompressor.nextValue();
            index--;
        }
        return value;
    }
    @Override
    public List<Double> decompress(CompressedChunk compressedChunk, int start, int end) {
        List<Double> values = new ArrayList<>(end - start);
        decompressor.refresh();
        decompressor.setBytes(compressedChunk.getCompressedBytes());
        int index = 0;
        while (index < start) {
            decompressor.nextValue();
            index++;
        }
        while (index < end) {
            values.add(decompressor.nextValue());
            index++;
        }
        return values;
    }

    @Override
    public List<Double> decompress(CompressedChunk compressedChunk) {
        List<Double> values = new ArrayList<>(compressedChunk.getDataNum());
        decompressor.refresh();
        decompressor.setBytes(compressedChunk.getCompressedBytes());
        for (int i = 0; i < compressedChunk.getDataNum(); i++) {
            values.add(decompressor.nextValue());
        }
        return values;
    }

    public final void setBytes(byte[] bs) {
        decompressor.setBytes(bs);
    }


//    public Double randomQuery(int dataIndex){
//        int blockFileIndex = randomQueryFindBlockFile(dataIndex);
//        return randomQueryInBlock(dataIndex,blockFileIndex);
//    }
//    public final Integer randomQueryFindBlockFile(int index){
//        int i;
//        for (i = 1; i < blockFiles.size(); i++){
//            String blockFileName = blockFiles.get(i).getName();
//            int blockIData = Integer.parseInt(blockFileName.substring(0, blockFileName.lastIndexOf('.')));
//            if (index < blockIData){
//                return i-1;
//            }
//        }
//        return blockFiles.size()-1;
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
//        int startBlockFileIndex = randomQueryFindBlockFile(startIndex);
//        int endBlockFileIndex = randomQueryFindBlockFile(endIndex);
//
//
//        if(startBlockFileIndex == endBlockFileIndex){
//            max = maxValueFormiToj(startIndex,endIndex,startBlockFileIndex);
//        }
//        else{
//            max = maxValueFromiToEnd(startIndex ,startBlockFileIndex);
//            Double max2 = maxValueForm0Toi(endIndex ,endBlockFileIndex);
//            if (max < max2){
//                max = max2;
//            }
//            maxValuesInBlocks = readMinMaxValuesFromFile(datasetFileName, "MaxValues.txt");
//            for (int i = startBlockFileIndex + 1; i < endBlockFileIndex; i++){
//                double maxInBlock = maxValuesInBlocks.get(i);
//                if (max < maxInBlock){
//                    max = maxInBlock;
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
//        int startBlockFileIndex = randomQueryFindBlockFile(startIndex);
//        int endBlockFileIndex = randomQueryFindBlockFile(endIndex);
//
//        if (startBlockFileIndex == endBlockFileIndex){
//            min = minValueFormiToj(startIndex,endIndex,startBlockFileIndex);
//        }
//        else {
//            min = minValueFromiToEnd(startIndex, startBlockFileIndex);
//            Double min2 = minValueForm0Toi(endIndex, endBlockFileIndex);
//            if (min > min2){
//                min = min2;
//            }
//            minValuesInBlocks = readMinMaxValuesFromFile(datasetFileName,"MinValues.txt");
//            for (int i = startBlockFileIndex + 1; i < endBlockFileIndex; i++){
//                double minInBlock = minValuesInBlocks.get(i);
//                if (min > minInBlock){
//                    min = minInBlock;
//                }
//            }
//        }
//        return min;
//    }
//
//    public List<Integer> rangeQuery(double f){
//        List<Integer> result = new ArrayList<>();
//        minValuesInBlocks = readMinMaxValuesFromFile(datasetFileName,"MinValues.txt");
//        maxValuesInBlocks = readMinMaxValuesFromFile(datasetFileName, "MaxValues.txt");
//        for (int i = 0 ; i < blockFiles.size();i++){
//            if (f <= maxValuesInBlocks.get(i) && f >= minValuesInBlocks.get(i)){
//                decompressor.refresh();
//                File file = blockFiles.get(i);
//                block.readFromFile(file);
//                decompressor.setBytes(block.getData());
//                //int indexOfFirstData = block.getIData();
//                int indexOfFirstData = Integer.parseInt(file.getName().substring(0, file.getName().lastIndexOf('.')));
//                int dataNumber = 0;
//                if (i < blockFiles.size() - 1){
//                    String nextBlockFileName = blockFiles.get(i+1).getName();
//                    int nextBlockIData = Integer.parseInt(nextBlockFileName.substring(0, nextBlockFileName.lastIndexOf('.')));
//                    if (i == 0) {
//                        indexOfFirstData = 0;
//                    }
//                    dataNumber = nextBlockIData - indexOfFirstData;
//                }
//                else if (i == blockFiles.size() - 1){
//                    dataNumber = block.readIDataFromFile(blockFiles.get(0)) - indexOfFirstData;
//                }
//                List<Double> floatings = decompress(dataNumber);
//                for (int j=0; j < floatings.size(); j++){
//                    if (Double.toString(f).equals(Double.toString(floatings.get(j)))){
//                        result.add(j + indexOfFirstData);
//                    }
//                }
//            }
//        }
//        return result;
//    }
//
//
//    private Double randomQueryInBlock(int dataIndex,int blockFileIndex){
//        decompressor.refresh();
//        File file = blockFiles.get(blockFileIndex);
//        block.readFromFile(file);
//        decompressor.setBytes(block.getData());
//        int dataIndexInBlock;
//        if (blockFileIndex == 0){
//            dataIndexInBlock = dataIndex;
//        }
//        else{
//            dataIndexInBlock = dataIndex - block.getIData();
//        }
//
//        List<Double> floatings = decompress(dataIndexInBlock+1);
//        return floatings.get(dataIndexInBlock);
//    }
//
//    private Double maxValueFromiToEnd(int fromIndex, int blockIndex){
//        decompressor.refresh();
//        block.readFromFile(blockFiles.get(blockIndex));
//        decompressor.setBytes(block.getData());
//        String blockFileName = blockFiles.get(blockIndex).getName();
//        int iData =Integer.parseInt(blockFileName.substring(0, blockFileName.lastIndexOf('.')));
//        fromIndex -= iData;
//        Double max = Double.MIN_VALUE;
//        Double value;
//        for (int i = 0; i < fromIndex; i ++){
//            decompressor.nextValue();
//        }
//        String nextBlockFileName = blockFiles.get(blockIndex+1).getName();
//        int nextBlockIData = Integer.parseInt(nextBlockFileName.substring(0, nextBlockFileName.lastIndexOf('.')));
//        int valuesNumber = nextBlockIData - block.getIData();
//        for (int i = fromIndex; i < valuesNumber; i++){
//            value = decompressor.nextValue();
//            if (value > max){
//                max = value;
//            }
//        }
//        return max;
//
//    }
//    private Double maxValueForm0Toi(int toIndex,int blockIndex){
//        decompressor.refresh();
//        block.readFromFile(blockFiles.get(blockIndex));
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
//    private Double maxValueFormiToj (int startIndex,int endIndex, int blockFileIndex){
//        decompressor.refresh();
//        block.readFromFile(blockFiles.get(blockFileIndex));
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
//    private Double minValueFromiToEnd(int fromIndex, int blockIndex){
//        decompressor.refresh();
//        block.readFromFile(blockFiles.get(blockIndex));
//        decompressor.setBytes(block.getData());
//        String blockFileName = blockFiles.get(blockIndex).getName();
//        int iData =Integer.parseInt(blockFileName.substring(0, blockFileName.lastIndexOf('.')));
//        fromIndex -= iData;
//        Double min = Double.MAX_VALUE;
//        for (int i = 0; i < fromIndex; i ++){
//            decompressor.nextValue();
//        }
//        String nextBlockFileName = blockFiles.get(blockIndex+1).getName();
//        int nextBlockIData = Integer.parseInt(nextBlockFileName.substring(0, nextBlockFileName.lastIndexOf('.')));
//        int valuesNumber = nextBlockIData - block.getIData();
//        for (int i = fromIndex; i < valuesNumber; i++){
//            Double value = decompressor.nextValue();
//            if (value < min){
//                min = value;
//            }
//        }
//        return min;
//    }
//
//    private Double minValueForm0Toi(int toIndex,int blockIndex){
//        decompressor.refresh();
//        block.readFromFile(blockFiles.get(blockIndex));
//        decompressor.setBytes(block.getData());
//        toIndex -= block.getIData();
//        Double min = Double.MAX_VALUE;
//        for (int i = 0; i <= toIndex; i++){
//            Double value = decompressor.nextValue();
//            if (value < min){
//                min = value;
//            }
//        }
//        return min;
//    }
//    private Double minValueFormiToj (int startIndex,int endIndex, int blockFileIndex){
//        decompressor.refresh();
//        block.readFromFile(blockFiles.get(blockFileIndex));
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
//    private List<File> readFilesFromFile(String datasetFileName) {
//        List<File> newFiles = new ArrayList<>();
//        File catalogFile = new File(folderPath_Bytes_Chunk + datasetFileName + "/" + "blockFiles.txt");
//        try (FileInputStream fileIn = new FileInputStream(catalogFile);
//             ObjectInputStream in = new ObjectInputStream(fileIn)) {
//            newFiles = (List<File>) in.readObject();
//        } catch (IOException | ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//        return newFiles;
//    }
//    private List<Double> readMinMaxValuesFromFile(String datasetFileName, String valueFileName) {
//        List<Double> values = new ArrayList<>();
//        File catalogFile = new File(folderPath_Bytes_Chunk + datasetFileName + "/" + valueFileName);
//        try (FileInputStream fileIn = new FileInputStream(catalogFile);
//             ObjectInputStream in = new ObjectInputStream(fileIn)) {
//            values = (List<Double>) in.readObject();
//        } catch (IOException | ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//        return values;
//    }


    public void refresh() {
        decompressor.refresh();
        block.refresh();
    }
}
