package org.urbcomp.startdb.selfstar.compressor;

import org.urbcomp.startdb.selfstar.compressor.xor.IXORCompressor;
import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.util.Arrays;

public class ElfCompressor implements ICompressor {
    private final IXORCompressor xorCompressor;
    private int compressedSizeInBits = 0;
    private OutputBitStream os;

    private int numberOfValues = 0;

    public ElfCompressor(IXORCompressor xorCompressor) {
        this.xorCompressor = xorCompressor;
        os = xorCompressor.getOutputStream();
    }

    @Override
    public void addValue(double v) {
        long vLong = Double.doubleToRawLongBits(v);
        long vPrimeLong;
        numberOfValues++;

        if (v == 0.0 || Double.isInfinite(v)) {
            compressedSizeInBits += os.writeBit(false);
            vPrimeLong = vLong;
        } else if (Double.isNaN(v)) {
            compressedSizeInBits += os.writeBit(false);
            vPrimeLong = 0x7ff8000000000000L;
        } else {
            int[] alphaAndBetaStar = Elf64Utils.getAlphaAndBetaStar(v);
            int e = ((int) (vLong >> 52)) & 0x7ff;
            int gAlpha = Elf64Utils.getFAlpha(alphaAndBetaStar[0]) + e - 1023;
            int eraseBits = 52 - gAlpha;
            long mask = 0xffffffffffffffffL << eraseBits;
            long delta = (~mask) & vLong;
            if (alphaAndBetaStar[1] < 16 && delta != 0 && eraseBits > 4) {
                compressedSizeInBits += os.writeInt(alphaAndBetaStar[1] | 0x10, 5);
                vPrimeLong = mask & vLong;
            } else {
                compressedSizeInBits += os.writeBit(false);
                vPrimeLong = vLong;
            }
        }
        compressedSizeInBits += xorCompressor.addValue(vPrimeLong);
    }

    @Override
    public byte[] getBytes() {
        return xorCompressor.getOut();
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
        // we write one more bit here, for marking an end of the stream.
        compressedSizeInBits += os.writeBit(false);  // case 0
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

    @Override
    public void refresh() {
        xorCompressor.refresh();
        compressedSizeInBits = 0;
        numberOfValues = 0;
        os = xorCompressor.getOutputStream();
    }
}
