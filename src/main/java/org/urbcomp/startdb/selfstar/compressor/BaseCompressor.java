package org.urbcomp.startdb.selfstar.compressor;

import org.urbcomp.startdb.selfstar.compressor.xor.IXORCompressor;

import java.util.Arrays;

public class BaseCompressor implements ICompressor {
    private final IXORCompressor xorCompressor;
    private int compressedSizeInBits = 0;
    private int numberOfValues = 0;

    public BaseCompressor(IXORCompressor xorCompressor) {
        this.xorCompressor = xorCompressor;
    }

    @Override
    public void addValue(double v) {
        numberOfValues++;
        compressedSizeInBits += xorCompressor.addValue(Double.doubleToRawLongBits(v));
    }

    @Override
    public byte[] getBytes() {
        int byteCount = (int) Math.ceil(compressedSizeInBits / 8.0);
        return Arrays.copyOf(xorCompressor.getOut(), byteCount);
    }

    @Override
    public byte[] getBytes(int start, int end){
        int startByteIndex = start / 8;
        int startBitIndex = start % 8;
        int endByteIndex = end / 8;
        int endBitIndex = end % 8;

        int numBitsToCopy = start - end;
        byte[] returnBytes = new byte[(int) Math.ceil(numBitsToCopy / 8.0)];
        byte[] Bits = xorCompressor.getOut();

        int copiedBitIndex = 0;
        for (int i=startByteIndex;i<=endByteIndex;i++){
            byte currentByte = Bits[i];
            int bitsToCopyInByte = (i == endByteIndex) ? endBitIndex : 8;
            for (int j = startBitIndex; j < bitsToCopyInByte; j++) {
                int bitValue = (currentByte >> (7 - j)) & 1;
                returnBytes[copiedBitIndex / 8] |= bitValue << (7 - (copiedBitIndex % 8));
                copiedBitIndex++;
            }
            startBitIndex = 0; // for subsequent bytes, start at the beginning
        }
        return returnBytes;
    }

    @Override
    public void close() {
        compressedSizeInBits += xorCompressor.close();
    }

    @Override
    public double getCompressionRatio() {
        return compressedSizeInBits / (numberOfValues * 64.0);
    }

    @Override
    public long getCompressedSizeInBits() {
        return compressedSizeInBits;
    }

    public String getKey() {
        return xorCompressor.getKey();
    }

    @Override
    public void refresh() {
        xorCompressor.refresh();
        compressedSizeInBits = 0;
        numberOfValues = 0;
    }
}
