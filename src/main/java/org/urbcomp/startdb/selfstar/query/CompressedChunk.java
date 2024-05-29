package org.urbcomp.startdb.selfstar.query;

public class CompressedChunk {
    private int iData;
    private int dataNum;
    private double minValue;
    private double maxValue;
    private int significanceBitSize;
    private byte[] compressedBytes;

    public CompressedChunk() {
        this.maxValue = Double.MIN_VALUE;
        this.minValue = Double.MAX_VALUE;
        this.significanceBitSize = 0;
        this.iData = 0;
        this.compressedBytes = null;
    }

    public CompressedChunk(byte[] cBytes, int iData, int dataNum, double minValue, double maxValue, int significanceBitSize) {
        setBytes(cBytes, significanceBitSize);
        this.iData = iData;
        this.dataNum = dataNum;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.significanceBitSize = significanceBitSize;
    }


    public void setBytes(byte[] cBytes, int significanceBitSize){
        int byteCount = significanceBitSize / 8; // Number of full bytes to copy
        int bitCount = significanceBitSize % 8;  // Remaining bits to copy
        // Copy full bytes
        System.arraycopy(cBytes, 0, this.compressedBytes, 0, byteCount);

        // Copy remaining bits
        if (bitCount > 0) {
            byte mask = (byte) (0xFF << (8 - bitCount)); // Create mask for the remaining bits
            this.compressedBytes[byteCount] = (byte) (cBytes[byteCount] & mask); // Copy remaining bits
        }
    }

    public int getIData() {
        return iData;
    }

    public int getDataNum() {
        return dataNum;
    }

    public double getMinValue() {
        return minValue;
    }

    public int getSignificanceBitSize() {
        return significanceBitSize;
    }

    public double getMaxValue() {
        return maxValue;
    }

    public byte[] getCompressedBytes() {
        return compressedBytes;
    }
}
