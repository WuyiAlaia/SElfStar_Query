package org.urbcomp.startdb.selfstar.decompressor.query;

import java.util.List;

public interface IQueryDecompressor {
    List<Double> decompress(int valuesNumber);
    Double RandomQuery (int index);

}
