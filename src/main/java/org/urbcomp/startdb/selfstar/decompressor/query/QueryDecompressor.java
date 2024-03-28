package org.urbcomp.startdb.selfstar.decompressor.query;

import org.apache.jena.base.Sys;
import org.urbcomp.startdb.selfstar.decompressor.IDecompressor;
import org.urbcomp.startdb.selfstar.query.CompressedBlock;
import org.urbcomp.startdb.selfstar.utils.BlockReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;


public class QueryDecompressor implements IQueryDecompressor{
    private IDecompressor decompressor;
    private List<CompressedBlock> compressedBlocks;

    public QueryDecompressor(IDecompressor decompressor, List<CompressedBlock> compressedBlocks) {
        this.decompressor = decompressor;
        this.compressedBlocks = compressedBlocks;
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
        for (int i = 0; i < compressedBlocks.size(); i++){
            if (compressedBlocks.get(i).getIData() > index){
                return RandomQueryInBlock(index,i-1);
            }
        }
        return RandomQueryInBlock(index,compressedBlocks.size()-1);
    }

    public Double MaxQuery(int start, int end){
        int startBlockIndex;
        int endBlockIndex;
        Double max;
        if(start > end){
            int tmp = start;
            start = end;
            end = tmp;
        }
        for (startBlockIndex = 0; startBlockIndex < compressedBlocks.size(); startBlockIndex++){
            if (compressedBlocks.get(startBlockIndex).getIData() > start){
                startBlockIndex--;
                break;
            }
        }
        for (endBlockIndex = startBlockIndex; endBlockIndex < compressedBlocks.size(); endBlockIndex++){
            if(compressedBlocks.get(endBlockIndex).getIData() > end){
                endBlockIndex--;
                break;
            }
            if(endBlockIndex == compressedBlocks.size()-1){
                break;
            }

        }

        if(startBlockIndex == endBlockIndex){
            max = MaxValueFormiToj(start,end,startBlockIndex);
        }
        else{
            max = MaxValueFromiToEnd(start ,startBlockIndex);
            Double max2 = MaxValueForm0Toi(end ,endBlockIndex);
            if (max < max2){
                max = max2;
            }
            for (int i = startBlockIndex + 1; i < endBlockIndex; i++){
                if (compressedBlocks.get(i).getMaxValue() > max){
                    max = compressedBlocks.get(i).getMaxValue();
                }
            }
        }
        return max;
    }

    public Double MinQuery(int start, int end){
        int startBlockIndex;
        int endBlockIndex;
        Double min;
        if(start > end){
            int tmp = start;
            start = end;
            end = tmp;
        }
        for (startBlockIndex = 0; startBlockIndex < compressedBlocks.size(); startBlockIndex++){
            if (compressedBlocks.get(startBlockIndex).getIData() > start){
                startBlockIndex--;
                break;
            }
        }
        for (endBlockIndex = startBlockIndex; endBlockIndex < compressedBlocks.size(); endBlockIndex++){
            if(compressedBlocks.get(endBlockIndex).getIData() > end){
                endBlockIndex--;
                break;
            }
            if(endBlockIndex == compressedBlocks.size()-1){
                break;
            }
        }

        if (startBlockIndex == endBlockIndex){
            min = MinValueFormiToj(start,end,startBlockIndex);
        }
        else {
            min = MinValueFromiToEnd(start, startBlockIndex);
            Double min2 = MinValueForm0Toi(end, endBlockIndex);
            if (min > min2){
                min = min2;
            }
            for (int i = startBlockIndex + 1; i < endBlockIndex; i++){
                if (compressedBlocks.get(i).getMinValue() < min){
                    min = compressedBlocks.get(i).getMinValue();
                }
            }
        }
        return min;
    }



    public Double RandomQueryInBlock(int DataIndex,int BlockIndex){
        decompressor.refresh();
        decompressor.setBytes(compressedBlocks.get(BlockIndex).getData());
        int indexinblock = DataIndex - compressedBlocks.get(BlockIndex).getIData();
//        Double value;
//        for (int i = 0; i < indexinblock ; i++){
//            value = decompressor.nextValue();
//            if (value == 1.0){
//                System.out.println("o");
//            }
//        }
//        return decompressor.nextValue();
        List<Double> floatings = decompress(indexinblock+1);
        return floatings.get(indexinblock);
    }

    public Double MaxValueFromiToEnd(int fromIndex, int BlockIndex){
        List<Double> floatings = readfile("Air-pressure.csv");

        decompressor.refresh();
        decompressor.setBytes(compressedBlocks.get(BlockIndex).getData());
        fromIndex -= compressedBlocks.get(BlockIndex).getIData();
        Double max = Double.MIN_VALUE;
        Double value;
        for (int i = 0; i < fromIndex; i ++){
            value = decompressor.nextValue();
            Double v = floatings.get(i + compressedBlocks.get(BlockIndex).getIData());
            if(value != v){
                System.out.println("error");
            }
        }
        int valuesNumber = compressedBlocks.get(BlockIndex+1).getIData() - compressedBlocks.get(BlockIndex).getIData();
        for (int i = fromIndex; i < valuesNumber; i++){
            value = decompressor.nextValue();
            if (value > max){
                max = value;
            }
        }
        return max;

    }

    public Double MaxValueForm0Toi(int toIndex,int BlockIndex){
        decompressor.refresh();
        decompressor.setBytes(compressedBlocks.get(BlockIndex).getData());
        toIndex -= compressedBlocks.get(BlockIndex).getIData();
        Double max = Double.MIN_VALUE;
        for (int i = 0; i <= toIndex; i++){
            Double value = decompressor.nextValue();
            if (value > max){
                max = value;
            }
        }
        return max;
    }
    public Double MaxValueFormiToj (int startindex,int endindex, int BlockIndex){
        decompressor.refresh();
        decompressor.setBytes(compressedBlocks.get(BlockIndex).getData());
        Double max = Double.MIN_VALUE;
        Double value;
        startindex -= compressedBlocks.get(BlockIndex).getIData();
        endindex -= compressedBlocks.get(BlockIndex).getIData();
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
    public Double MinValueFromiToEnd(int fromIndex, int BlockIndex){
        decompressor.refresh();
        decompressor.setBytes(compressedBlocks.get(BlockIndex).getData());
        fromIndex -= compressedBlocks.get(BlockIndex).getIData();
        Double min = Double.MAX_VALUE;
        for (int i = 0; i < fromIndex; i ++){
            decompressor.nextValue();
        }
        int valuesNumber = compressedBlocks.get(BlockIndex+1).getIData() - compressedBlocks.get(BlockIndex).getIData();
        for (int i = fromIndex; i < valuesNumber; i++){
            Double value = decompressor.nextValue();
            if (value < min){
                min = value;
            }
        }
        return min;
    }

    public Double MinValueForm0Toi(int toIndex,int BlockIndex){
        decompressor.refresh();
        decompressor.setBytes(compressedBlocks.get(BlockIndex).getData());
        toIndex -= compressedBlocks.get(BlockIndex).getIData();
        Double min = Double.MAX_VALUE;
        for (int i = 0; i <= toIndex; i++){
            Double value = decompressor.nextValue();
            if (value < min){
                min = value;
            }
        }
        return min;
    }

    public Double MinValueFormiToj (int startindex,int endindex, int BlockIndex){
        decompressor.refresh();
        decompressor.setBytes(compressedBlocks.get(BlockIndex).getData());
        Double min = Double.MAX_VALUE;
        Double value;
        startindex -= compressedBlocks.get(BlockIndex).getIData();
        endindex -= compressedBlocks.get(BlockIndex).getIData();
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
