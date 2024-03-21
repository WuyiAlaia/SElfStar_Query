package org.urbcomp.startdb.selfstar.query;


import org.urbcomp.startdb.selfstar.compressor.ICompressor;
import org.urbcomp.startdb.selfstar.decompressor.IDecompressor;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;
import java.util.Arrays;

import java.util.ArrayList;
import java.util.List;

public class CompressAndQuery {
    private ICompressor compressor;
    private String fileName;
    private  List<CompressedBlock> compressedBlocks;
    private OutputBitStream os;

    public CompressAndQuery(ICompressor compressor,String filename){
        this(compressor,filename,1000); //The default window size of OutputBitStream is 1000
    }

    public CompressAndQuery(ICompressor compressor,String filename,int osWindows){
        this.compressor = compressor;
        this.fileName = filename;
        compressedBlocks = new ArrayList<>();
        os = new OutputBitStream(
                new byte[(int) (((osWindows + 1) * 8 + osWindows / 8 + 1) * 1.2)]);
        Chunk();
    }

    public void Chunk(){
        int currentDataIndex = 0;  //current index in data flow
        int currentBlockIndex = -1;
        int currentBitIndex = 0;   //current bit index in Block

        try (BlockReader br = new BlockReader(fileName, 1000)){
            List<Double> floatings;
            boolean blockIfFull = true; //if current block is full
            while ((floatings = br.nextBlock()) != null){
                for (Double floating : floatings) {

                    long beforeAddValueBits = compressor.getCompressedSizeInBits();
                    compressor.addValue(floating);
                    long afterAddValueBits = compressor.getCompressedSizeInBits();
                    System.out.println("beforeAddValueBits:"+beforeAddValueBits+"  "+"afterAddValueBits:"+afterAddValueBits);
                    byte[] writtenBits = compressor.getBytes((int)beforeAddValueBits,(int)afterAddValueBits);   //writtenBits = buffer[beforeAddValueBits,afterAddValueBits)
                    int writtenBitsSize = (int)(afterAddValueBits-beforeAddValueBits);
                    System.out.println("CompressedSizeInBits:"+compressor.getCompressedSizeInBits()+"  "+"writtenBitSize:"+writtenBitsSize+"  "+"writtenBits:"+Arrays.toString(writtenBits));
                    System.out.println("compress.buffer:"+Arrays.toString(compressor.getBytes()));
                    //If block is full || If remaining space has some space/bits but not enough
                    if(blockIfFull || writtenBitsSize > compressedBlocks.get(compressedBlocks.size()-1).getRemainingSize()){
                        currentBlockIndex++;
                        compressedBlocks.add(new CompressedBlock());
                        compressedBlocks.get(currentBlockIndex).resetIData(currentDataIndex);
                        compressor.refresh();
                        blockIfFull = false;
                        currentBitIndex = 0;

                        compressor.addValue(floating);
                        int initWrittenBitsSize = (int)compressor.getCompressedSizeInBits();
                        byte[] initWrittenBits = compressor.getBytes(0,initWrittenBitsSize);
                        compressedBlocks.get(currentBlockIndex).writeData(initWrittenBits,initWrittenBitsSize,currentBitIndex);
                        currentBitIndex += initWrittenBitsSize;
                    }
                    //write bits into Block's data[]
                    else{
                        compressedBlocks.get(currentBlockIndex).writeData(writtenBits,writtenBitsSize,currentBitIndex);
                        currentBitIndex += writtenBitsSize;

                        //reset Max,Min,remainingSize
                        if(floating > compressedBlocks.get(currentBlockIndex).getMaxValue()){
                            compressedBlocks.get(currentBlockIndex).resetMaxValue(floating);
                        }
                        else if(floating < compressedBlocks.get(currentBlockIndex).getMinValue()){
                            compressedBlocks.get(currentBlockIndex).resetMinValue(floating);
                        }
                        int updateRemainingSize = compressedBlocks.get(currentBlockIndex).getRemainingSize() - writtenBitsSize;
                        compressedBlocks.get(currentBlockIndex).resetRemainingSize(updateRemainingSize);

                        if(updateRemainingSize == 0){
                            blockIfFull = true;
                        }
                    }
                    System.out.println("Blocksss.buffer:"+Arrays.toString(compressedBlocks.get(currentBlockIndex).getData()));

                    currentDataIndex++;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(fileName, e);
        }

    }

    public Double RandomQuery(IDecompressor decompressor,int index){
        for (int i=0; i < compressedBlocks.size(); i++){
            if (compressedBlocks.get(i).getIData() > index){
                decompressor.setBytes(compressedBlocks.get(i-1).getData());
                List<Double> deValues = decompressor.decompress();
                //System.out.println(deValues);
                return deValues.get(index - compressedBlocks.get(i-1).getIData());
            }
        }
        return null;
    }

    public Double MaxQuery(IDecompressor decompressor, int start, int end){
        Double max = Double.MIN_VALUE;
        int startBlockIndex;
        int endBlockIndex;
        for (startBlockIndex = 0; startBlockIndex < compressedBlocks.size(); startBlockIndex++){
            if (compressedBlocks.get(startBlockIndex).getIData() > start){
                startBlockIndex--;
                break;
            }
        }
        for (endBlockIndex = 0; endBlockIndex < compressedBlocks.size(); endBlockIndex++){
            if(compressedBlocks.get(endBlockIndex).getIData() > end){
                endBlockIndex--;
                break;
            }
        }


        for (int i=startBlockIndex+1; i<endBlockIndex ; i++){
            if (compressedBlocks.get(i).getMaxValue() > max){
                max = compressedBlocks.get(i).getMaxValue();
            }
        }

        decompressor.setBytes(compressedBlocks.get(startBlockIndex).getData());
        List<Double> deValue1 = decompressor.decompress();
        for (int i=start-compressedBlocks.get(startBlockIndex).getIData(); i < deValue1.size(); i++){
            if (deValue1.get(i) > max){
                max = deValue1.get(i);
            }
        }

        decompressor.refresh();
        decompressor.setBytes(compressedBlocks.get(endBlockIndex).getData());
        List<Double> deValue2 = decompressor.decompress();
        for (int i = 0; i <= end-compressedBlocks.get(endBlockIndex).getIData(); i++){
            if (deValue2.get(i) > max){
                max = deValue2.get(i);
            }
        }

        return max;

    }

    public Double MinQuery(IDecompressor decompressor, int start, int end){
        Double min = Double.MAX_VALUE;
        int startBlockIndex;
        int endBlockIndex;
        for (startBlockIndex = 0; startBlockIndex < compressedBlocks.size(); startBlockIndex++){
            if (compressedBlocks.get(startBlockIndex).getIData() > start){
                startBlockIndex--;
                break;
            }
        }
        for (endBlockIndex = 0; endBlockIndex < compressedBlocks.size(); endBlockIndex++){
            if(compressedBlocks.get(endBlockIndex).getIData() > end){
                endBlockIndex--;
                break;
            }
        }


        for (int i=startBlockIndex+1; i<endBlockIndex ; i++){
            if (compressedBlocks.get(i).getMinValue() < min){
                min = compressedBlocks.get(i).getMinValue();
            }
        }

        decompressor.setBytes(compressedBlocks.get(startBlockIndex).getData());
        List<Double> deValue1 = decompressor.decompress();
        for (int i=start-compressedBlocks.get(startBlockIndex).getIData(); i < deValue1.size(); i++){
            if (deValue1.get(i) < min){
                min = deValue1.get(i);
            }
        }

        decompressor.refresh();
        decompressor.setBytes(compressedBlocks.get(endBlockIndex).getData());
        List<Double> deValue2 = decompressor.decompress();
        for (int i = 0; i <= end-compressedBlocks.get(endBlockIndex).getIData(); i++){
            if (deValue2.get(i) < min){
                min = deValue2.get(i);
            }
        }

        return min;

    }


}
