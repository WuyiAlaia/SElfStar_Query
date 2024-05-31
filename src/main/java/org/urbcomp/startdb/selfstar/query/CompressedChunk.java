package org.urbcomp.startdb.selfstar.query;


import java.io.*;
import java.util.List;

public class CompressedChunk {
    private int iData;
    private int dataNum;
    private double minValue;
    private double maxValue;
    private int significanceBitSize;
    private byte[] compressedBytes;


    public CompressedChunk() {
        this.maxValue = Double.MIN_VALUE;
        this.minValue = Double.MAX_VALUE;
        this.significanceBitSize = 0;
        this.iData = 0;
        this.compressedBytes = null;
    }

    public CompressedChunk(byte[] cBytes, int iData, int dataNum, double minValue, double maxValue, int significanceBitSize) {
        setBytes(cBytes, significanceBitSize);
        this.iData = iData;
        this.dataNum = dataNum;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.significanceBitSize = significanceBitSize;
    }


    public void setBytes(byte[] cBytes, int significanceBitSize) {
        int byteCount = significanceBitSize / 8; // Number of full bytes to copy
        int bitCount = significanceBitSize % 8;  // Remaining bits to copy
        // Copy full bytes
        System.arraycopy(cBytes, 0, this.compressedBytes, 0, byteCount);

        // Copy remaining bits
        if (bitCount > 0) {
            byte mask = (byte) (0xFF << (8 - bitCount)); // Create mask for the remaining bits
            this.compressedBytes[byteCount] = (byte) (cBytes[byteCount] & mask); // Copy remaining bits
        }
    }


    public int getIData() {
        return iData;
    }

    public int getDataNum() {
        return dataNum;
    }

    public double getMinValue() {
        return minValue;
    }

    public int getSignificanceBitSize() {
        return significanceBitSize;
    }

    public double getMaxValue() {
        return maxValue;
    }

    public byte[] getCompressedBytes() {
        return compressedBytes;
    }

    public static CompressedChunk fromBytes(byte[] bytes) {
        // Implement deserialization from bytes
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        try (ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (CompressedChunk) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    public byte[] toBytes() {
        // Implement serialization to bytes
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(this);
            oos.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void writeChunkToFile(CompressedChunk chunk, String filename) {
        try (FileOutputStream fileOut = new FileOutputStream(filename);
            ObjectOutputStream objectOut = new ObjectOutputStream(fileOut)) {
            objectOut.writeInt(chunk.getIData());
            objectOut.writeInt(chunk.getDataNum());
            objectOut.writeDouble(chunk.getMinValue());
            objectOut.writeDouble(chunk.getMaxValue());
            objectOut.writeInt(chunk.getSignificanceBitSize());
            byte[] bytes = chunk.getCompressedBytes();
            objectOut.writeInt(bytes.length);
            objectOut.write(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
