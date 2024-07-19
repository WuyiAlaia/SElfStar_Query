package org.urbcomp.startdb.selfstar.compressor.query;

import com.github.Cwida.alp.ALPCompression;
import org.urbcomp.startdb.selfstar.query.MetaData;

import java.util.ArrayList;
import java.util.List;

public class QueryALP {
    private ALPCompression compressor;
    private final int blockDataCapacity;
    private int currentDataIndex = 0;

    private final List<byte[]> chunkList = new ArrayList<>();

    private final List<MetaData> metaDataList = new ArrayList<>();

    public QueryALP(int blockSize) {
        this.blockDataCapacity = blockSize * 8;
        compressor = new ALPCompression(blockDataCapacity);
    }



}
