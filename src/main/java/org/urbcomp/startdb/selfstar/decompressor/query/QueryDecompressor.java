package org.urbcomp.startdb.selfstar.decompressor.query;

import org.urbcomp.startdb.selfstar.decompressor.IDecompressor;
import org.urbcomp.startdb.selfstar.query.CompressedBlock;
import org.urbcomp.startdb.selfstar.utils.BlockReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;


public class QueryDecompressor implements IQueryDecompressor{
    private static final String folderPath_Bytes_Chunk = "D:/bytes/ChunkBytes/";
    private final IDecompressor decompressor;
    private final CompressedBlock block = new CompressedBlock();
    private final List<File> blockFiles;

    public QueryDecompressor(IDecompressor decompressor, List<File> blockFiles) {
        this.decompressor = decompressor;
        this.blockFiles = blockFiles;
        // this.blockFiles = readFilesFromFile(datasetFileName);
    }


    public List<Double> decompress(int valuesNumber) {
        List<Double> values = new ArrayList<>(valuesNumber);

        for (int i = 0; i < valuesNumber; i++) {
            values.add(decompressor.nextValue());
        }
        return values;
    }

    public final void setBytes(byte[] bs){
        decompressor.setBytes(bs);
    }



    public Double randomQuery(int dataIndex){
        int blockFileIndex = randomQueryFindBlockFile(dataIndex);
        return randomQueryInBlock(dataIndex,blockFileIndex);
    }
    public final Integer randomQueryFindBlockFile(int index){
        int i;
        for (i = 1; i < blockFiles.size(); i++){
            String blockFileName = blockFiles.get(i).getName();
            int blockIData = Integer.parseInt(blockFileName.substring(0, blockFileName.lastIndexOf('.')));
            if (index < blockIData){
                return i-1;
            }
        }
        return blockFiles.size()-1;
    }

    public Double maxQuery(int startIndex, int endIndex){
        if (startIndex == endIndex){
            return randomQuery(startIndex);
        }
        Double max;
        if(startIndex > endIndex){
            int tmp = startIndex;
            startIndex = endIndex;
            endIndex = tmp;
        }
        int startBlockFileIndex = randomQueryFindBlockFile(startIndex);
        int endBlockFileIndex = randomQueryFindBlockFile(endIndex);


        if(startBlockFileIndex == endBlockFileIndex){
            max = maxValueFormiToj(startIndex,endIndex,startBlockFileIndex);
        }
        else{
            max = maxValueFromiToEnd(startIndex ,startBlockFileIndex);
            Double max2 = maxValueForm0Toi(endIndex ,endBlockFileIndex);
            if (max < max2){
                max = max2;
            }
            for (int i = startBlockFileIndex + 1; i < endBlockFileIndex; i++){
                double maxInBlock = block.readMaxValueFromFile(blockFiles.get(i));
                if (max < maxInBlock){
                    max = maxInBlock;
                }
            }
        }
        return max;
    }
    public Double minQuery(int startIndex, int endIndex){
        if (startIndex == endIndex){
            return randomQuery(startIndex);
        }
        Double min;
        if(startIndex > endIndex){
            int tmp = startIndex;
            startIndex = endIndex;
            endIndex = tmp;
        }
        int startBlockFileIndex = randomQueryFindBlockFile(startIndex);
        int endBlockFileIndex = randomQueryFindBlockFile(endIndex);

        if (startBlockFileIndex == endBlockFileIndex){
            min = minValueFormiToj(startIndex,endIndex,startBlockFileIndex);
        }
        else {
            min = minValueFromiToEnd(startIndex, startBlockFileIndex);
            Double min2 = minValueForm0Toi(endIndex, endBlockFileIndex);
            if (min > min2){
                min = min2;
            }
            for (int i = startBlockFileIndex + 1; i < endBlockFileIndex; i++){
                double minInBlock = block.readMinValueFromFile(blockFiles.get(i));
                if (min > minInBlock){
                    min = minInBlock;
                }
            }
        }
        return min;
    }

    public List<Integer> rangeQuery(double f){
        List<Integer> result = new ArrayList<>();
        for (int i = 0 ; i < blockFiles.size();i++){
            File file = blockFiles.get(i);
            if (f <= block.readMaxValueFromFile(file) && f >= block.readMinValueFromFile(file)){
                decompressor.refresh();
                block.readFromFile(file);
                decompressor.setBytes(block.getData());
                //int indexOfFirstData = block.getIData();
                int indexOfFirstData = Integer.parseInt(file.getName().substring(0, file.getName().lastIndexOf('.')));
                int dataNumber = 0;
                if (i < blockFiles.size() - 1){
                    String nextBlockFileName = blockFiles.get(i+1).getName();
                    int nextBlockIData = Integer.parseInt(nextBlockFileName.substring(0, nextBlockFileName.lastIndexOf('.')));
                    if (i == 0) {
                        indexOfFirstData = 0;
                    }
                    dataNumber = nextBlockIData - indexOfFirstData;
                }
                else if (i == blockFiles.size() - 1){
                    dataNumber = block.readIDataFromFile(blockFiles.get(0)) - indexOfFirstData;
                }
                List<Double> floatings = decompress(dataNumber);
                for (int j=0; j < floatings.size(); j++){
                    if (Double.toString(f).equals(Double.toString(floatings.get(j)))){
                        result.add(j + indexOfFirstData);
                    }
                }
            }
        }
        return result;
    }


    private Double randomQueryInBlock(int dataIndex,int blockFileIndex){
        decompressor.refresh();
        File file = blockFiles.get(blockFileIndex);
        block.readFromFile(file);
        decompressor.setBytes(block.getData());
        int dataIndexInBlock;
        if (blockFileIndex == 0){
            dataIndexInBlock = dataIndex;
        }
        else{
            dataIndexInBlock = dataIndex - block.getIData();
        }

        List<Double> floatings = decompress(dataIndexInBlock+1);
        return floatings.get(dataIndexInBlock);
    }

    private Double maxValueFromiToEnd(int fromIndex, int blockIndex){
        decompressor.refresh();
        block.readFromFile(blockFiles.get(blockIndex));
        decompressor.setBytes(block.getData());
        fromIndex -= block.getIData();
        Double max = Double.MIN_VALUE;
        Double value;
        for (int i = 0; i < fromIndex; i ++){
            decompressor.nextValue();
        }
        String nextBlockFileName = blockFiles.get(blockIndex+1).getName();
        int nextBlockIData = Integer.parseInt(nextBlockFileName.substring(0, nextBlockFileName.lastIndexOf('.')));
        int valuesNumber = nextBlockIData - block.getIData();
        for (int i = fromIndex; i < valuesNumber; i++){
            value = decompressor.nextValue();
            if (value > max){
                max = value;
            }
        }
        return max;

    }
    private Double maxValueForm0Toi(int toIndex,int blockIndex){
        decompressor.refresh();
        block.readFromFile(blockFiles.get(blockIndex));
        decompressor.setBytes(block.getData());
        toIndex -= block.getIData();
        Double max = Double.MIN_VALUE;
        for (int i = 0; i <= toIndex; i++){
            Double value = decompressor.nextValue();
            if (value > max){
                max = value;
            }
        }
        return max;
    }
    private Double maxValueFormiToj (int startIndex,int endIndex, int blockFileIndex){
        decompressor.refresh();
        block.readFromFile(blockFiles.get(blockFileIndex));
        decompressor.setBytes(block.getData());
        Double max = Double.MIN_VALUE;
        Double value;
        startIndex -= block.getIData();
        endIndex -= block.getIData();
        for (int i = 0; i < startIndex; i++){
            decompressor.nextValue();
        }
        for (int i = startIndex; i <= endIndex; i++){
            value = decompressor.nextValue();
            if (value > max){
                max = value;
            }
        }
        return max;
    }
    private Double minValueFromiToEnd(int fromIndex, int blockIndex){
        decompressor.refresh();
        block.readFromFile(blockFiles.get(blockIndex));
        decompressor.setBytes(block.getData());
        fromIndex -= block.getIData();
        Double min = Double.MAX_VALUE;
        for (int i = 0; i < fromIndex; i ++){
            decompressor.nextValue();
        }
        String nextBlockFileName = blockFiles.get(blockIndex+1).getName();
        int nextBlockIData = Integer.parseInt(nextBlockFileName.substring(0, nextBlockFileName.lastIndexOf('.')));
        int valuesNumber = nextBlockIData - block.getIData();
        for (int i = fromIndex; i < valuesNumber; i++){
            Double value = decompressor.nextValue();
            if (value < min){
                min = value;
            }
        }
        return min;
    }

    private Double minValueForm0Toi(int toIndex,int blockIndex){
        decompressor.refresh();
        block.readFromFile(blockFiles.get(blockIndex));
        decompressor.setBytes(block.getData());
        toIndex -= block.getIData();
        Double min = Double.MAX_VALUE;
        for (int i = 0; i <= toIndex; i++){
            Double value = decompressor.nextValue();
            if (value < min){
                min = value;
            }
        }
        return min;
    }
    private Double minValueFormiToj (int startIndex,int endIndex, int blockFileIndex){
        decompressor.refresh();
        block.readFromFile(blockFiles.get(blockFileIndex));
        decompressor.setBytes(block.getData());
        Double min = Double.MAX_VALUE;
        Double value;
        startIndex -= block.getIData();
        endIndex -= block.getIData();
        for (int i = 0; i < startIndex; i++){
            decompressor.nextValue();
        }
        for (int i = startIndex; i <= endIndex; i++){
            value = decompressor.nextValue();
            if (value < min){
                min = value;
            }
        }
        return min;
    }

    private List<File> readFilesFromFile(String datasetFileName) {
        List<File> newFiles = new ArrayList<>();
        File catalogFile = new File(folderPath_Bytes_Chunk + datasetFileName + "/" + "blockFiles");
        try (FileInputStream fileIn = new FileInputStream(catalogFile);
             ObjectInputStream in = new ObjectInputStream(fileIn)) {
            newFiles = (List<File>) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return newFiles;
    }

    public void refresh(){
        decompressor.refresh();
        block.refresh();
    }
}
