package org.urbcomp.startdb.selfstar.compressor.query;

import org.urbcomp.startdb.selfstar.compressor.ICompressor;
import org.urbcomp.startdb.selfstar.utils.BlockReader;
import org.urbcomp.startdb.selfstar.query.CompressedBlock;

import java.util.ArrayList;
import java.util.List;

public class QueryCompressor implements IQueryCompressor{
    private ICompressor compressor;
    private String fileName;
    private List<CompressedBlock> compressedBlocks;
    private int blockDataCapacity;

    public QueryCompressor(ICompressor compressor, String filename){
        this(compressor,filename,1024);
    }

    public QueryCompressor(ICompressor compressor, String filename, int blockdatabitsize){
        this.compressor = compressor;
        this.fileName = filename;
        compressedBlocks = new ArrayList<>();
        this.blockDataCapacity = blockdatabitsize;
        Chunk();
    }

    public void Chunk(){
        int currentDataIndex = 0;
        int currentDataNumber = 0;
        int currentBlockIndex = -1;
        long currentBitSize = 0;
        try (BlockReader br = new BlockReader(fileName,1000)){
            List<Double> floatings;
            boolean blockIfFull = true;
            while ((floatings = br.nextBlock()) != null){
                for (int i = 0; i < floatings.size(); i++){
                    double floating = floatings.get(i);
                    long beforeAddValueBitsSize = compressor.getCompressedSizeInBits();
                    compressor.addValue(floating);
                    currentDataNumber++;
                    currentBitSize = compressor.getCompressedSizeInBits();
                    if (blockIfFull || currentBitSize > blockDataCapacity){
                        //write data into current CompressedBlock
                        currentDataNumber--;
                        if (currentBlockIndex != -1) {
                            //write data[] and WrittenBitSize
                            compressedBlocks.get(currentBlockIndex).writeData(compressor.getBytes(),beforeAddValueBitsSize);
                            compressedBlocks.get(currentBlockIndex).resetWrittenBitSize(beforeAddValueBitsSize);
                            compressedBlocks.get(currentBlockIndex).resetDataNumber(currentDataNumber);
                            currentDataNumber = 0;
                        }

                        //add a new CompressedBlock
                        currentBlockIndex++;
                        currentDataNumber++;
                        compressedBlocks.add(new CompressedBlock(currentBlockIndex,blockDataCapacity / 8 + 1));
                        compressedBlocks.get(currentBlockIndex).resetIData(currentDataIndex);
                        compressedBlocks.get(currentBlockIndex).resetMaxValue(floating);
                        compressedBlocks.get(currentBlockIndex).resetMinValue(floating);
                        compressor.refresh();
                        compressor.addValue(floating);
                        blockIfFull = false;
                    }
                    else {
                        if(floating > compressedBlocks.get(currentBlockIndex).getMaxValue()){
                            compressedBlocks.get(currentBlockIndex).resetMaxValue(floating);
                        }
                        else if(floating < compressedBlocks.get(currentBlockIndex).getMinValue()){
                            compressedBlocks.get(currentBlockIndex).resetMinValue(floating);
                        }

                        if(currentBitSize == blockDataCapacity){
                            blockIfFull = true;
                        }
                    }

                    currentDataIndex++;
                }
            }
            // write into the last part
            long beforeAddValueBitsSize = compressor.getCompressedSizeInBits();
            compressor.addValue(88.88888888) ; //解决最后的byte为写满的问题
            compressedBlocks.get(currentBlockIndex).writeData(compressor.getBytes(),beforeAddValueBitsSize);
            compressedBlocks.get(currentBlockIndex).resetWrittenBitSize(beforeAddValueBitsSize);
            compressedBlocks.get(currentBlockIndex).resetDataNumber(currentDataNumber);

        }catch (Exception e) {
            throw new RuntimeException(fileName, e);
        }
    }

    public List<CompressedBlock> getCompressedBlocks(){
        return this.compressedBlocks;
    }
}
