package org.urbcomp.startdb.selfstar.utils;

import org.urbcomp.startdb.selfstar.query.CompressedBlock;

import java.io.*;

public class BlockFileReader {

    public void writeToFile (CompressedBlock block,File file){
        try (FileOutputStream fileOut = new FileOutputStream(file);
             ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            out.writeDouble(block.getMinValue());
            out.writeDouble(block.getMaxValue());
            out.writeInt(block.getIData());
            out.writeLong(block.getWrittenBitSize());
            out.writeObject(block.getData());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void readFromFile(File file) {
        try (FileInputStream fileIn = new FileInputStream(file);
             ObjectInputStream in = new ObjectInputStream(fileIn)) {
            double minValue = in.readDouble();
            double maxValue = in.readDouble();
            int iData = in.readInt();
            long writtenBitSize = in.readLong();
            byte[] data = (byte[]) in.readObject();
            System.out.println(minValue);
            System.out.println(maxValue);
            System.out.println(iData);
            System.out.println(writtenBitSize);
            System.out.println(data.length);
            for (byte b : data) {
                System.out.print(b + " ");
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public boolean clearFile(File file){
        try (FileOutputStream fos = new FileOutputStream(file)) {
            // Do nothing, just opening the file in non-append mode will clear it
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (file.length() == 0) {
            return true;
        } else {
            return false;
        }
    }
}
