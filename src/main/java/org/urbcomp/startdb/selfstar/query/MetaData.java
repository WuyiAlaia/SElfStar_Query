package org.urbcomp.startdb.selfstar.query;

import java.io.Serializable;

public class MetaData implements Serializable {
    private final int firstValueIndex;
    private int dataNum;
    private double maxValue;
    private double minValue;
    private double sum;

    public MetaData(int firstValueIndex) {
        this.firstValueIndex = firstValueIndex;
    }

    public void setMaxValue(double maxValue) {
        this.maxValue = maxValue;
    }

    public void setMinValue(double minValue) {
        this.minValue = minValue;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public void setDataNum(int dataNum) {
        this.dataNum = dataNum;
    }

    public int getFirstValueIndex() {
        return firstValueIndex;
    }

    public int getDataNum() {
        return dataNum;
    }

    public double getMaxValue() {
        return maxValue;
    }

    public double getMinValue() {
        return minValue;
    }

    public double getSum() {
        return sum;
    }

    public int getEndIndex() {
        return this.firstValueIndex + this.dataNum;
    }
}
