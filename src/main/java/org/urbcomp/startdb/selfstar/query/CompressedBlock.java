package org.urbcomp.startdb.selfstar.query;


import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.io.*;
import java.util.Date;
import java.util.Arrays;

public class CompressedBlock {
    //metadata
    private int iData;
    private double minValue;
    private double maxValue;
    private  long writtenBitSize;
    private  byte[] data;

    public CompressedBlock(){
        this.maxValue = Double.MIN_VALUE;
        this.minValue = Double.MAX_VALUE;
        this.writtenBitSize = 0;
        this.iData = 0;
        this.data = null;
    }

    public double getMinValue() {
        return minValue;
    }
    public void resetMinValue(Double value){minValue = value;}

    public double getMaxValue() {
        return maxValue;
    }
    public void resetMaxValue(Double value){maxValue = value;}
    public long getWrittenBitSize(){
        return writtenBitSize;
    }
    public void resetWrittenBitSize(long size) { writtenBitSize = size;}
    public int getIData() {return iData;}
    public void resetIData(int dataIndex){iData = dataIndex;}


    public void resetData(byte[] writtenBits, long writtenBitsSize){
        int writtenByteSize = (int) writtenBitsSize / 8;
        int lastBitSize = (int) writtenBitsSize % 8;
        byte[] data = new byte[writtenByteSize+1];
        for (int i = 0; i < writtenByteSize; i++){
            data[i] = writtenBits[i];
        }

        if (lastBitSize != 0){
            byte b = writtenBits[writtenByteSize];
            for (int i = 0; i < lastBitSize; i++){
                int bit = (b >> (7-i)) & 1;
                data[writtenByteSize] |= bit << (7 - i);
            }
        }

        this.data = data;
    }

    public byte[] getData(){
        if (writtenBitSize % 8 == 0){
            return Arrays.copyOfRange(data, 0, (int)writtenBitSize / 8);
        }
        else{
            return Arrays.copyOfRange(data, 0, (int)writtenBitSize / 8 + 1);
        }
    }

    public void writeToFile (File file){
        try (FileOutputStream fileOut = new FileOutputStream(file);
             ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            out.writeDouble(minValue);
            out.writeDouble(maxValue);
            out.writeInt(iData);
            out.writeLong(writtenBitSize);
            out.writeObject(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void readFromFile(File file) {
        try (FileInputStream fileIn = new FileInputStream(file);
             ObjectInputStream in = new ObjectInputStream(fileIn)) {
            this.minValue = in.readDouble();
            this.maxValue = in.readDouble();
            this.iData = in.readInt();
            this.writtenBitSize = in.readLong();
            this.data = (byte[]) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    public Double readMinValueFromFile (File file){
        try (FileInputStream fileIn = new FileInputStream(file);
             ObjectInputStream in = new ObjectInputStream(fileIn)) {
            double min = in.readDouble();
            return min;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Double readMaxValueFromFile (File file){
        try (FileInputStream fileIn = new FileInputStream(file);
             ObjectInputStream in = new ObjectInputStream(fileIn)) {
            double max = in.readDouble();
            max = in.readDouble();
            return max;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    public Integer readIDataFromFile(File file){
        try (FileInputStream fileIn = new FileInputStream(file);
             ObjectInputStream in = new ObjectInputStream(fileIn)) {
            in.readDouble();
            in.readDouble();
            int iData = in.readInt();
            return iData;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void refresh(){
        this.maxValue = Double.MIN_VALUE;
        this.minValue = Double.MAX_VALUE;
        this.writtenBitSize = 0;
        this.iData = 0;
        this.data = null;
    }
}
