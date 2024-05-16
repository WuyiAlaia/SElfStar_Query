package org.urbcomp.startdb.selfstar.query;

public class metaData {
    private int iData;
    private int maxValue;
    private int minValue;

    public metaData(int iData,int minValue,int maxValue){
        this.iData = iData;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }
    public metaData (){
        this.iData = 0;
        this.maxValue = 0;
    }

    public int getiData() {return iData;}

}
