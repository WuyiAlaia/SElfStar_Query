package org.urbcomp.startdb.selfstar.compressor.query;

import org.urbcomp.startdb.selfstar.compressor.ICompressor;
import org.urbcomp.startdb.selfstar.query.MetaData;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class QueryCompressor implements IQueryCompressor {
    private ICompressor compressor;
    private final int blockDataCapacity;

    private int currentDataIndex = 0;

    private final List<byte[]> chunkList = new ArrayList<>();
    private final List<MetaData> metaDataList = new ArrayList<>();

    private final ThreadPoolExecutor globalThreadPool = new ThreadPoolExecutor(6, 12, 10, java.util.concurrent.TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    public QueryCompressor(ICompressor compressor) {
        this(compressor, 1024);
    }

    public QueryCompressor(ICompressor compressor, int blockSize) {
        this.compressor = compressor;
        this.blockDataCapacity = blockSize * 8;
    }

    public void compressAll(List<Double> allDoubles) {
        while (currentDataIndex < allDoubles.size() - 1) {
            MetaData currentChunkMetaData = new MetaData(currentDataIndex);
            int dataNum = 0;
            long lastChunkLen;
            double minValue = Double.MAX_VALUE;
            double maxValue = Double.NEGATIVE_INFINITY;
            double sum = 0;
            while (true) {
                lastChunkLen = compressor.getCompressedSizeInBits();
                compressor.addValue(allDoubles.get(currentDataIndex));
                if (compressor.getCompressedSizeInBits() > blockDataCapacity) {
//                    System.out.println("compress bit size is too large, current data index: " + currentDataIndex);
                    currentDataIndex--;
                    break;
                }

                minValue = Math.min(minValue, allDoubles.get(currentDataIndex));
                maxValue = Math.max(maxValue, allDoubles.get(currentDataIndex));
                sum += allDoubles.get(currentDataIndex);
                currentDataIndex++;
                dataNum++;
                if (currentDataIndex >= allDoubles.size()) {
                    lastChunkLen = compressor.getCompressedSizeInBits();
                    compressor.addValue(Double.NaN);
//                    System.out.println("compress data size is too large, current data index: " + currentDataIndex);
                    break;
                }
            }
            byte[] compressedBytes = Arrays.copyOf(compressor.getBytes(), (int) Math.ceil(lastChunkLen / 8.0));

            compressor.refresh();
            currentChunkMetaData.setDataNum(dataNum);
            currentChunkMetaData.setMinValue(minValue);
            currentChunkMetaData.setMaxValue(maxValue);
            currentChunkMetaData.setSum(sum);
            chunkList.add(compressedBytes);
            metaDataList.add(currentChunkMetaData);
        }
    }

    public void saveToFile(String id) {
        for (int i = 0; i < chunkList.size(); i++) {
            globalThreadPool.execute(new ChunkConsistencyTask(chunkList.get(i), id, i));
        }
        try {
            ObjectOutputStream metaDataOutputStream = new ObjectOutputStream(Files.newOutputStream(new File(id + "_meta.dat").toPath()));
            metaDataOutputStream.writeInt(metaDataList.size());
            for (MetaData metaData : metaDataList) {
                metaDataOutputStream.writeObject(metaData);
            }
            metaDataOutputStream.flush();
            metaDataOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<MetaData> getMetaDataList() {
        return metaDataList;
    }

    public List<byte[]> getChunkList() {
        return chunkList;
    }

    class ChunkConsistencyTask implements Runnable {
        private final byte[] chunk;
        private final String id;
        private final int index;

        public ChunkConsistencyTask(byte[] chunk, String id, int index) {
            this.chunk = chunk;
            this.id = id;
            this.index = index;
        }

        @Override
        public void run() {
            try {
                BufferedOutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(new File(id + "_" + index + ".dat").toPath()));
                outputStream.write(chunk);
                outputStream.flush();
                outputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
