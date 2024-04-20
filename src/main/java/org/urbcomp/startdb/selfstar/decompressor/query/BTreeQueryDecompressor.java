package org.urbcomp.startdb.selfstar.decompressor.query;

import org.apache.jena.base.Sys;
import org.urbcomp.startdb.selfstar.decompressor.IDecompressor;
import org.urbcomp.startdb.selfstar.query.BTree;
import org.urbcomp.startdb.selfstar.query.CompressedBlock;
import org.urbcomp.startdb.selfstar.utils.BlockReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;


public class BTreeQueryDecompressor implements IQueryDecompressor{
    private IDecompressor decompressor;
    private BTree compressedBlocksTree;

    public BTreeQueryDecompressor(IDecompressor decompressor, BTree compressedBlocksTree) {
        this.decompressor = decompressor;
        this.compressedBlocksTree = compressedBlocksTree;
    }


    public List<Double> decompress(int valuesNumber) {

        List<Double> values = new ArrayList<>(valuesNumber);

        for (int i = 0; i < valuesNumber; i++) {
            values.add(decompressor.nextValue());
        }
        return values;
    }
    public void setBytes(byte[] bs){
        decompressor.setBytes(bs);
    }

    public Double RandomQuery(int index){
        return RandomQueryInBlock(index,compressedBlocksTree.findWithDataIndex(index));
    }

    public Double MaxQuery(int start, int end){
        CompressedBlock startBlock = compressedBlocksTree.findWithDataIndex(start);
        CompressedBlock endBlock = compressedBlocksTree.findWithDataIndex(end);
        Double max;
        if(start > end){
            int tmp = start;
            start = end;
            end = tmp;
        }

        if(startBlock.getIBlock() == endBlock.getIBlock()){
            max = MaxValueFormiToj(start,end,startBlock);
        }
        else{
            max = MaxValueFromiToEnd(start ,startBlock);
            Double max2 = MaxValueForm0Toi(end ,endBlock);
            if (max < max2){
                max = max2;
            }
            for (int i = startBlock.getIBlock() + 1; i < endBlock.getIBlock(); i++){
                if (compressedBlocksTree.findWithBlockIndex(i).getMaxValue() > max){
                    max = compressedBlocksTree.findWithBlockIndex(i).getMaxValue();
                }
            }
        }
        return max;
    }

    public Double MinQuery(int start, int end){
        CompressedBlock startBlock = compressedBlocksTree.findWithDataIndex(start);
        CompressedBlock endBlock = compressedBlocksTree.findWithDataIndex(end);
        Double min;
        if(start > end){
            int tmp = start;
            start = end;
            end = tmp;
        }

        if (startBlock.getIBlock() == endBlock.getIBlock()){
            min = MinValueFormiToj(start,end,startBlock);
        }
        else {
            min = MinValueFromiToEnd(start, startBlock);
            Double min2 = MinValueForm0Toi(end, endBlock);
            if (min > min2){
                min = min2;
            }
            for (int i = startBlock.getIBlock() + 1; i < endBlock.getIBlock(); i++){
                if (compressedBlocksTree.findWithBlockIndex(i).getMinValue() < min){
                    min = compressedBlocksTree.findWithBlockIndex(i).getMinValue();
                }
            }
        }
        return min;
    }



    public Double RandomQueryInBlock(int DataIndex,CompressedBlock block){
        decompressor.refresh();
        decompressor.setBytes(block.getData());
        int indexinblock = DataIndex - block.getIData();
        List<Double> floatings = decompress(indexinblock+1);
        return floatings.get(indexinblock);
    }

    public Double MaxValueFromiToEnd(int fromIndex, CompressedBlock block){
        List<Double> floatings = readfile("Air-pressure.csv");

        decompressor.refresh();
        decompressor.setBytes(block.getData());
        fromIndex -= block.getIData();
        Double max = Double.MIN_VALUE;
        Double value;
        for (int i = 0; i < fromIndex; i ++){
            value = decompressor.nextValue();
            Double v = floatings.get(i + block.getIData());
            if(value != v){
                System.out.println("error");
            }
        }
        int valuesNumber = block.getDataNumber();
        for (int i = fromIndex; i < valuesNumber; i++){
            value = decompressor.nextValue();
            if (value > max){
                max = value;
            }
        }
        return max;

    }

    public Double MaxValueForm0Toi(int toIndex,CompressedBlock block){
        decompressor.refresh();
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
    public Double MaxValueFormiToj (int startindex,int endindex, CompressedBlock block){
        decompressor.refresh();
        decompressor.setBytes(block.getData());
        Double max = Double.MIN_VALUE;
        Double value;
        startindex -= block.getIData();
        endindex -= block.getIData();
        for (int i = 0; i < startindex; i++){
            decompressor.nextValue();
        }
        for (int i = startindex; i <= endindex; i++){
            value = decompressor.nextValue();
            if (value > max){
                max = value;
            }
        }
        return max;
    }
    public Double MinValueFromiToEnd(int fromIndex, CompressedBlock block){
        decompressor.refresh();
        decompressor.setBytes(block.getData());
        fromIndex -= block.getIData();
        Double min = Double.MAX_VALUE;
        for (int i = 0; i < fromIndex; i ++){
            decompressor.nextValue();
        }
        int valuesNumber = block.getDataNumber();
        for (int i = fromIndex; i < valuesNumber; i++){
            Double value = decompressor.nextValue();
            if (value < min){
                min = value;
            }
        }
        return min;
    }

    public Double MinValueForm0Toi(int toIndex,CompressedBlock block){
        decompressor.refresh();
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

    public Double MinValueFormiToj (int startindex,int endindex, CompressedBlock block){
        decompressor.refresh();
        decompressor.setBytes(block.getData());
        Double min = Double.MAX_VALUE;
        Double value;
        startindex -= block.getIData();
        endindex -= block.getIData();
        for (int i = 0; i < startindex; i++){
            decompressor.nextValue();
        }
        for (int i = startindex; i <= endindex; i++){
            value = decompressor.nextValue();
            if (value < min){
                min = value;
            }
        }
        return min;
    }


    public List<Double> readfile(String filename) {
        List<Double> floatings = new ArrayList<>();
        try (BlockReader br = new BlockReader(filename, 1000)) {
            List<Double> tmp;
            while ((tmp = br.nextBlock()) != null){
                floatings.addAll(tmp);
            }
        } catch (Exception e) {
            throw new RuntimeException(filename, e);
        }
        return floatings;
    }

    public void refresh(){
        decompressor.refresh();
    }
}
