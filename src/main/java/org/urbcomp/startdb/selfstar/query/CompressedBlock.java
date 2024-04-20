package org.urbcomp.startdb.selfstar.query;


import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.util.Date;
import java.util.Arrays;

public class CompressedBlock {
    //metadata
    private final int iBlock;
    private  int DataNumber;
    private int iData;
    private double minValue;
    private double maxValue;
    private  long writtenBitSize;
    private byte[] data;

    public CompressedBlock(final int BlockIndex, final int datasize){
        iBlock = BlockIndex;
        maxValue = Double.MIN_VALUE;
        minValue = Double.MAX_VALUE;
        DataNumber = 0;
        writtenBitSize = 0;
        iData = 0;
        data = new byte[datasize];
    }

    public double getMinValue() {
        return minValue;
    }
    public void resetMinValue(Double value){minValue = value;}

    public double getMaxValue() {
        return maxValue;
    }
    public void resetMaxValue(Double value){maxValue = value;}
    public long getWrittenBitSize(){
        return writtenBitSize;
    }
    public void resetWrittenBitSize(long size) { writtenBitSize = size;}
    public int getDataNumber(){
        return DataNumber;
    }
    public void resetDataNumber(int datanumber){DataNumber = datanumber;}
    public int getIData() {return iData;}
    public void resetIData(int dataIndex){iData = dataIndex;}
    public int getIBlock() {return iBlock;}


    public void writeData(byte[] writtenBits,long writtenBitsSize, int currentBitIndex){
        long writtenByteIndex = writtenBitsSize / 8;
        long writtenBitIndex = writtenBitsSize % 8;
        int DataByteIndex = currentBitIndex / 8;
        int DataBitIndex = currentBitIndex % 8;

        long writtenByteSize = (writtenBitIndex == 0) ? writtenByteIndex : writtenByteIndex+1;
        for (int i = 0; i < writtenByteSize ; i++){
            byte writtenByte = writtenBits[i];
            long bitsToWrittenInByte = (i == writtenByteIndex) ? writtenBitIndex : 8;
            for (int j = 0; j < bitsToWrittenInByte; j++){
                int bit = (writtenByte >> (7 - j)) & 1;
                data[DataByteIndex] |= bit << (7 - DataBitIndex);
                DataBitIndex++;
                if (DataBitIndex == 8){
                    DataBitIndex = 0;
                    DataByteIndex++;
                }
            }
        }
    }

    public void writeData(byte[] writtenBits, long writtenBitsSize){
        int writtenByteSize = (int) writtenBitsSize / 8;
        int lastBitSize = (int) writtenBitsSize % 8;
        for (int i = 0; i < writtenByteSize; i++){
            data[i] = writtenBits[i];
        }

        byte b = writtenBits[writtenByteSize];
        for (int i = 0; i < lastBitSize; i++){
            int bit = (b >> (7-i)) & 1;
            data[writtenByteSize] |= bit << (7 - lastBitSize);
        }
    }

    public byte[] getData(){
        if (writtenBitSize % 8 == 0){
            return Arrays.copyOfRange(data, 0, (int)writtenBitSize / 8);
        }
        else{
            return Arrays.copyOfRange(data, 0, (int)writtenBitSize / 8 + 1);
        }
    }
}
