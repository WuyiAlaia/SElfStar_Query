package org.urbcomp.startdb.selfstar.query;


import java.util.Date;

public class CompressedBlock {

    //metadata
    private double minValue;
    private double maxValue;
    //    private double firstOriginalValue;
    private  int iData; //current index in data flow
    private int remainingSize;
    private byte[] data;

    public CompressedBlock(){
        this(1024);
    }
    public CompressedBlock(final int dataSize){
        maxValue = Double.MIN_VALUE;
        minValue = Double.MAX_VALUE;
//        firstOriginalValue = 0;
        iData = 0;
        remainingSize = dataSize;
        data = new byte[dataSize];
    }

    public double getMinValue() {
        return minValue;
    }
    public void resetMinValue(Double value){minValue = value;}

    public double getMaxValue() {
        return maxValue;
    }
    public void resetMaxValue(Double value){maxValue = value;}
    public int getRemainingSize(){
        return remainingSize;
    }
    public void resetRemainingSize(int newRemainingSize){remainingSize = newRemainingSize;}
    //    public double getFirstOriginalValue() {
//        return firstOriginalValue;
//    }
//    public void resetFirstOriginalValue(Double value){firstOriginalValue = value;}
    public int getIData(){
        return iData;
    }
    public void resetIData(int dataIndex){iData = dataIndex;}
    public void writeData(byte[] writtenBits,int writtenBitsSize, int currentBitIndex){
        int writtenByteIndex = writtenBitsSize / 8;
        int writtenBitIndex = writtenBitsSize % 8;
        int DataByteIndex = currentBitIndex / 8;
        int DataBitIndex = currentBitIndex % 8;

        for (int i = 0; i < writtenByteIndex ; i++){
            byte writtenByte = writtenBits[i];
            int bitsToWrittenInByte = (i == writtenByteIndex) ? writtenBitIndex : 8;
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

    public byte[] getData(){
        return data;
    }
}
