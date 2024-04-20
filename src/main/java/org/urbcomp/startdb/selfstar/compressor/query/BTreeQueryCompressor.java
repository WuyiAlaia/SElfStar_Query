package org.urbcomp.startdb.selfstar.compressor.query;

import org.urbcomp.startdb.selfstar.compressor.ICompressor;
import org.urbcomp.startdb.selfstar.utils.BlockReader;
import org.urbcomp.startdb.selfstar.query.CompressedBlock;
import org.urbcomp.startdb.selfstar.query.BTree;

import java.util.ArrayList;
import java.util.List;

public class BTreeQueryCompressor implements IQueryCompressor{
    private ICompressor compressor;
    private String fileName;
    private BTree compressedBlocksTree;
    private int blockDataCapacity;

    public BTreeQueryCompressor(ICompressor compressor, String filename){
        this(compressor,filename,1024);
    }

    public BTreeQueryCompressor(ICompressor compressor, String filename, int blockdatabitsize){
        this.compressor = compressor;
        this.fileName = filename;
        compressedBlocksTree = new BTree(5);
        this.blockDataCapacity = blockdatabitsize;
        Chunk();
    }

    public void Chunk(){
        int currentDataIndex = 0;
        int currentDataNumber = 0;
        int currentBlockIndex = -1;
        long currentBitSize = 0;
        CompressedBlock newCompressedBlock = null;
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
                        if (currentBlockIndex != -1) {
                            //write data[] and WrittenBitSize
                            newCompressedBlock.writeData(compressor.getBytes(),beforeAddValueBitsSize);
                            newCompressedBlock.resetWrittenBitSize(beforeAddValueBitsSize);
                            newCompressedBlock.resetDataNumber(currentDataNumber);
                            currentDataNumber = 0;
                            compressedBlocksTree.insert(newCompressedBlock);
                        }

                        //add a new CompressedBlock
                        currentBlockIndex++;
                        newCompressedBlock = new CompressedBlock(currentBlockIndex,blockDataCapacity / 8 + 1);
                        newCompressedBlock.resetIData(currentDataIndex);
                        newCompressedBlock.resetMaxValue(floating);
                        newCompressedBlock.resetMinValue(floating);
                        compressor.refresh();
                        compressor.addValue(floating);
                        blockIfFull = false;
                    }
                    else {
                        if(floating > newCompressedBlock.getMaxValue()){
                            newCompressedBlock.resetMaxValue(floating);
                        }
                        else if(floating < newCompressedBlock.getMinValue()){
                            newCompressedBlock.resetMinValue(floating);
                        }

                        if(currentBitSize == blockDataCapacity){
                            blockIfFull = true;
                        }
                    }

                    currentDataIndex++;
                }
            }
        }catch (Exception e) {
            throw new RuntimeException(fileName, e);
        }
    }

    public BTree getCompressedBlocksBTree(){
        return this.compressedBlocksTree;
    }
}
