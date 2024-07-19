package org.urbcomp.startdb.selfstar.query;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Filter;

public class Rosetta {
    private List<BloomFilter<Double>> bloomFilterList;
    private int level;

    public Rosetta(int level) {
        bloomFilterList = new ArrayList<>();
        this.level = level;
    }

}
