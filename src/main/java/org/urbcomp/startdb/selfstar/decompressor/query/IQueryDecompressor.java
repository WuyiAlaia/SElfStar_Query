package org.urbcomp.startdb.selfstar.decompressor.query;

import java.util.List;

public interface IQueryDecompressor {
    List<Double> decompress(int valuesNumber);
    Double randomQuery (int index);

    List<Integer> rangeQuery (double f);

    Double minQuery(int startIndex, int endIndex);

    Double maxQuery(int startIndex, int endIndex);


}
