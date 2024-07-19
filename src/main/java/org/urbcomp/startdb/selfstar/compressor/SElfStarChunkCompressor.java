package org.urbcomp.startdb.selfstar.compressor;

import org.urbcomp.startdb.selfstar.compressor.xor.IXORCompressor;
import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.util.Arrays;

public class SElfStarChunkCompressor implements ICompressor {
    private final IXORCompressor xorCompressor;

    private OutputBitStream os;

    private int compressedSizeInBits = 0;

    private int lastBetaStar = Integer.MAX_VALUE;

    private int numberOfValues = 0;


    public SElfStarChunkCompressor(IXORCompressor xorCompressor) {
        this.xorCompressor = xorCompressor;
        os = xorCompressor.getOutputStream();
    }

    public void addValue(double v) {
        if (numberOfValues % 1000 == 0)
            update();
        long vLong = Double.doubleToRawLongBits(v);
        long vPrimeLong;
        numberOfValues++;

        if (v == 0.0 || Double.isInfinite(v)) {
            compressedSizeInBits += os.writeInt(2, 2); // case 10
            vPrimeLong = vLong;
        } else if (Double.isNaN(v)) {
            compressedSizeInBits += os.writeInt(2, 2); // case 10
            vPrimeLong = 0x7ff8000000000000L;
        } else {
            // C1: v is a normal or subnormal
            int[] alphaAndBetaStar = Elf64Utils.getAlphaAndBetaStar(v, lastBetaStar);
            int e = ((int) (vLong >> 52)) & 0x7ff;
            int gAlpha = Elf64Utils.getFAlpha(alphaAndBetaStar[0]) + e - 1023;
            int eraseBits = 52 - gAlpha;
            long mask = 0xffffffffffffffffL << eraseBits;
            long delta = (~mask) & vLong;
            if (delta != 0 && eraseBits > 4) {  // C2
                if (alphaAndBetaStar[1] == lastBetaStar) {
                    compressedSizeInBits += os.writeBit(false);    // case 0
                } else {
                    compressedSizeInBits += os.writeInt(alphaAndBetaStar[1] | 0x30, 6);  // case 11, 2 + 4 = 6
                    lastBetaStar = alphaAndBetaStar[1];
                }
                vPrimeLong = mask & vLong;
            } else {
                compressedSizeInBits += os.writeInt(2, 2); // case 10
                vPrimeLong = vLong;
            }
        }
        compressedSizeInBits += xorCompressor.addValue(vPrimeLong);
    }

    public double getCompressionRatio() {
        return compressedSizeInBits / (numberOfValues * 64.0);
    }

    @Override
    public long getCompressedSizeInBits() {
        return compressedSizeInBits;
    }

    public byte[] getBytes() {

        int byteCount = (int) Math.ceil(compressedSizeInBits / 8.0);
        return Arrays.copyOf(xorCompressor.getOut(), byteCount);
    }

    @Override
    public byte[] getBytes(int start, int end) {
        int startByteIndex = start / 8;
        int startBitIndex = start % 8;
        int endByteIndex = end / 8;
        int endBitIndex = end % 8;

        int numBitsToCopy = end - start;
        byte[] returnBytes = new byte[(int) Math.ceil(numBitsToCopy / 8.0)];
        byte[] Bits = xorCompressor.getOut();

        int copiedBitIndex = 0;
        for (int i = startByteIndex; i <= endByteIndex; i++) {
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
    public void setDistribution(int[] leadDistribution, int[] trailDistribution) {
        // for streaming scenarios, we do nothing here
    }


    //todo: delete this method
    public void close() {

        xorCompressor.setDistribution(null, null);

        // we write one more bit here, for marking an end of the stream.
        compressedSizeInBits += os.writeInt(2, 2);  // case 10
        compressedSizeInBits += xorCompressor.close();
    }

    public void update() {
        xorCompressor.setDistribution(null, null);
    }


    public String getKey() {
        return xorCompressor.getKey();
    }

    public void refresh() {
        compressedSizeInBits = 0;
        lastBetaStar = Integer.MAX_VALUE;
        numberOfValues = 0;

        xorCompressor.refresh();        // note this refresh should be at the last
        os = xorCompressor.getOutputStream();
    }
}
