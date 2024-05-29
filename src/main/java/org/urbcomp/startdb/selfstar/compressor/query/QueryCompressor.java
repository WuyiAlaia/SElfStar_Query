package org.urbcomp.startdb.selfstar.compressor.query;

import org.urbcomp.startdb.selfstar.compressor.ICompressor;
import org.urbcomp.startdb.selfstar.query.CompressedChunk;
import org.urbcomp.startdb.selfstar.utils.BlockReader;
import org.urbcomp.startdb.selfstar.query.CompressedBlock;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

public class QueryCompressor implements IQueryCompressor {
    private ICompressor compressor;
    private final int blockDataCapacity;

    private long lastBitsSize = 0;
    private int currentDataIndex = 0;
    private int dataNum = 0;

    private double minValue = Double.MAX_VALUE;
    private double maxValue = Double.MIN_VALUE;

    private List<CompressedChunk> chunks = new ArrayList<>();

    public QueryCompressor(ICompressor compressor, String filename) {
        this(compressor, 1024);
    }

    public QueryCompressor(ICompressor compressor, int blockSize) {
        this.compressor = compressor;
        this.blockDataCapacity = blockSize * 8;
    }

    public void compress(double value) {
        compressor.addValue(value);
        long bitSize = compressor.getCompressedSizeInBits();
        if (bitSize > blockDataCapacity) {
            //write data[] and WrittenBitSize
            CompressedChunk compressedChunk = new CompressedChunk(compressor.getBytes(), currentDataIndex, dataNum, minValue, maxValue, (int) lastBitsSize);
            chunks.add(compressedChunk);
            compressor.refresh();
            compressor.addValue(value);
            dataNum = 1;
            bitSize = compressor.getCompressedSizeInBits();
            minValue = maxValue = value;
        }
        lastBitsSize = bitSize;
        dataNum++;
        currentDataIndex++;
        if (value > maxValue) {
            maxValue = value;
        }
        if (value < minValue) {
            minValue = value;
        }
    }




//
//    public List<File> getBlockFiles() {
//        return blockFiles;
//    }
//
//    // 将文件列表写入文件
//    private void writeFilesToFile(List<File> blockFiles, String datasetFile) {
//        File folder = new File(folderPath_Bytes_Chunk + datasetFile + "/");
//        if (!folder.exists()) {
//            folder.mkdirs();
//        }
//        File catalogFile = new File(folderPath_Bytes_Chunk + datasetFile + "/" + "blockFiles");
//        try {
//            if (!catalogFile.exists()) {
//                catalogFile.createNewFile();
//            } else {
//                boolean ifClear = clearFile(catalogFile);
//                if (!ifClear) {
//                    System.out.println("Fail to clear the file");
//                }
//            }
//        } catch (IOException e) {
//            System.out.println("Fail to create the file");
//            e.printStackTrace();
//        }
//
//        try (FileOutputStream fileOut = new FileOutputStream(catalogFile);
//             ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
//            out.writeObject(blockFiles);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//
//    }
//
//    private File createFiles(int iData, String datasetFile) {
//        File folder = new File(folderPath_Bytes_Chunk + datasetFile + "/");
//        if (!folder.exists()) {
//            folder.mkdirs();
//        }
//
//        String fileName = iData + ".txt";
//        File file = new File(folderPath_Bytes_Chunk + datasetFile + "/" + fileName);
//        try {
//            if (!file.exists()) {
//                file.createNewFile();
//            } else {
//                boolean ifClear = clearFile(file);
//                if (!ifClear) {
//                    System.out.println("Fail to clear the file: " + fileName);
//                }
//            }
//        } catch (IOException e) {
//            System.out.println("Fail to create the file:" + fileName);
//            e.printStackTrace();
//        }
//
//        return file;
//
//    }
//
//    private boolean clearFile(File file) {
//        try (FileOutputStream fos = new FileOutputStream(file)) {
//            // Do nothing, just opening the file in non-append mode will clear it
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        if (file.length() == 0) {
//            return true;
//        } else {
//            return false;
//        }
//    }
}
