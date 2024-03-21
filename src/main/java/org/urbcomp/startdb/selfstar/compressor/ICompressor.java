package org.urbcomp.startdb.selfstar.compressor;

public interface ICompressor {
    void addValue(double v);

    byte[] getBytes();

    byte[] getBytes(int start, int end);

    void close();

    double getCompressionRatio();

    long getCompressedSizeInBits();

    default String getKey() {
        return getClass().getSimpleName();
    }

    void refresh();

    default void setDistribution(int[] distribution1, int[] distribution2) {
    }
}
