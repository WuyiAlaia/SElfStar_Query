package org.urbcomp.startdb.selfstar.decompressor.query;

import org.urbcomp.startdb.selfstar.query.CompressedChunk;

import java.util.List;

public interface IQueryDecompressor {
    /***
     * Decompress a single value from a compressed chunk.
     * @param compressedChunk
     * @param index
     * @return
     */
    double decompress(CompressedChunk compressedChunk, int index);

    /***
     * Decompress a range of values from a compressed chunk.
     * @param compressedChunk
     * @param startIndex
     * @param endIndex
     * @return
     */
    List<Double> decompress(CompressedChunk compressedChunk, int startIndex, int endIndex);

    /***
     * Decompress all values from a compressed chunk.
     * @param compressedChunk
     * @return
     */
    List<Double> decompress(CompressedChunk compressedChunk);


}
