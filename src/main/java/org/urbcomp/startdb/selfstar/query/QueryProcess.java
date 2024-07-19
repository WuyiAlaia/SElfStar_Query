package org.urbcomp.startdb.selfstar.query;

import org.urbcomp.startdb.selfstar.decompressor.SElfStarDecompressorChunk;
import org.urbcomp.startdb.selfstar.decompressor.query.QueryDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.xor.SElfStarXORDecompressor;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class QueryProcess {

    public double max(String id, int start, int end) {
        QueryDecompressor decompressor = new QueryDecompressor(new SElfStarDecompressorChunk(new SElfStarXORDecompressor()));
        decompressor.loadMetaData(id);
        List<MetaData> metaDataList = decompressor.getMetaDataList();
        double maxValue = Double.NEGATIVE_INFINITY;
        int startChunkIndex = -1;
        int endChunkIndex = -1;
        for (int i = 0; i < metaDataList.size(); i++) {
            if (isInChunk(start, metaDataList.get(i))) startChunkIndex = i;
            if (isInChunk(end, metaDataList.get(i))) {
                endChunkIndex = i;
                break;
            }
        }
        if (startChunkIndex == -1 || endChunkIndex == -1)
            return Double.NaN;

//        for(int i = startChunkIndex; i <= endChunkIndex; i++) {
//            decompressor.loadChunk(id, i);
//        }
//        Map<Integer, ByteBuffer> chunkMap = decompressor.getChunkMap();
        if (startChunkIndex == endChunkIndex) {
            decompressor.loadChunk(id, startChunkIndex);
            List<Double> chunkValues = decompressor.decompress(decompressor.getChunkMap().get(startChunkIndex).array(), metaDataList.get(startChunkIndex).getDataNum());
            for (int i = start - metaDataList.get(startChunkIndex).getFirstValueIndex(); i <= end - metaDataList.get(endChunkIndex).getFirstValueIndex(); i++) {
                maxValue = Math.max(maxValue, chunkValues.get(i));
            }
            return maxValue;
        }

        for (int i = startChunkIndex + 1; i < endChunkIndex; i++) {
            maxValue = Math.max(maxValue, metaDataList.get(i).getMaxValue());
        }

        if (maxValue < metaDataList.get(startChunkIndex).getMaxValue()) {
            decompressor.loadChunk(id, startChunkIndex);
            List<Double> leftChunkValues = decompressor.decompress(decompressor.getChunkMap().get(startChunkIndex).array(), metaDataList.get(startChunkIndex).getDataNum());
            for (int i = start - metaDataList.get(startChunkIndex).getFirstValueIndex(); i < leftChunkValues.size(); i++) {
//                System.out.println("a[i] = " + i + " " + leftChunkValues.get(i));
                maxValue = Math.max(maxValue, leftChunkValues.get(i));
            }
        }

        if (maxValue < metaDataList.get(endChunkIndex).getMaxValue()) {
            decompressor.loadChunk(id, endChunkIndex);
            List<Double> rightChunkValues = decompressor.decompress(decompressor.getChunkMap().get(endChunkIndex).array(), metaDataList.get(endChunkIndex).getDataNum());
            for (int i = 0; i <= end - metaDataList.get(endChunkIndex).getFirstValueIndex(); i++) {
//                System.out.println("a[i] = " + i + " " + rightChunkValues.get(i));
                maxValue = Math.max(maxValue, rightChunkValues.get(i));
            }
        }

        return maxValue;
    }

    public double min(String id, int start, int end) {
        QueryDecompressor decompressor = new QueryDecompressor(new SElfStarDecompressorChunk(new SElfStarXORDecompressor()));
        decompressor.loadMetaData(id);
        List<MetaData> metaDataList = decompressor.getMetaDataList();
        double minValue = Double.MAX_VALUE;
        int startChunkIndex = -1;
        int endChunkIndex = -1;

        for (int i = 0; i < metaDataList.size(); i++) {
            if (isInChunk(start, metaDataList.get(i))) startChunkIndex = i;
            if (isInChunk(end, metaDataList.get(i))) {
                endChunkIndex = i;
                break;
            }
        }

        if (startChunkIndex == -1 || endChunkIndex == -1) return Double.NaN;
        for(int i = startChunkIndex; i <= endChunkIndex; i++) {
            decompressor.loadChunk(id, i);
        }
        Map<Integer, ByteBuffer> chunkMap = decompressor.getChunkMap();
        if (startChunkIndex == endChunkIndex) {
            List<Double> chunkValues = decompressor.decompress(chunkMap.get(startChunkIndex).array(), metaDataList.get(startChunkIndex).getDataNum());
            for (int i = start - metaDataList.get(startChunkIndex).getFirstValueIndex(); i <= end - metaDataList.get(endChunkIndex).getFirstValueIndex(); i++) {
                minValue = Math.min(minValue, chunkValues.get(i));
            }
            return minValue;
        }

        for (int i = startChunkIndex + 1; i < endChunkIndex; i++) {
            minValue = Math.min(minValue, metaDataList.get(i).getMinValue());
        }

        if (minValue > metaDataList.get(startChunkIndex).getMinValue()) {
            List<Double> leftChunkValues = decompressor.decompress(chunkMap.get(startChunkIndex).array(), metaDataList.get(startChunkIndex).getDataNum());
            for (int i = start - metaDataList.get(startChunkIndex).getFirstValueIndex(); i < leftChunkValues.size(); i++) {
                minValue = Math.min(minValue, leftChunkValues.get(i));
            }
        }

        if (minValue > metaDataList.get(startChunkIndex).getMinValue()) {
            List<Double> rightChunkValues = decompressor.decompress(chunkMap.get(endChunkIndex).array(), metaDataList.get(startChunkIndex).getDataNum());
            for (int i = 0; i < end - metaDataList.get(endChunkIndex).getFirstValueIndex(); i++) {
                minValue = Math.min(minValue, rightChunkValues.get(i));
            }
        }

        return minValue;
    }

    public double sum(String id, int start, int end) {
        QueryDecompressor decompressor = new QueryDecompressor(new SElfStarDecompressorChunk(new SElfStarXORDecompressor()));
        decompressor.loadMetaData(id);
        List<MetaData> metaDataList = decompressor.getMetaDataList();
        double sumValue = 0;
        int startChunkIndex = -1;
        int endChunkIndex = -1;
        for (MetaData metaData : metaDataList) {
            if (isInChunk(start, metaData)) {
                startChunkIndex = metaData.getFirstValueIndex();
            }
            if (isInChunk(end, metaData)) {
                endChunkIndex = metaData.getFirstValueIndex();
                break;
            }
        }
        if (startChunkIndex == -1 || endChunkIndex == -1) return 0;
        for(int i = startChunkIndex; i <= endChunkIndex; i++) {
            decompressor.loadChunk(id, i);
        }
        Map<Integer, ByteBuffer> chunkMap = decompressor.getChunkMap();

        if (startChunkIndex == endChunkIndex) {
            List<Double> chunkValues = decompressor.decompress(chunkMap.get(startChunkIndex).array(), metaDataList.get(startChunkIndex).getDataNum());
            for (int i = start - metaDataList.get(startChunkIndex).getFirstValueIndex(); i <= end - metaDataList.get(endChunkIndex).getFirstValueIndex(); i++) {
//                System.out.println("a[i] = " + i + " " + chunkValues.get(i));
                sumValue += chunkValues.get(i);
            }
            return sumValue;
        }

        for (int i = startChunkIndex + 1; i < endChunkIndex; i++) {
            sumValue += metaDataList.get(i).getSum();
        }

        List<Double> leftChunkValues = decompressor.decompress(chunkMap.get(startChunkIndex).array(), metaDataList.get(startChunkIndex).getDataNum());
        for (int i = start - metaDataList.get(startChunkIndex).getFirstValueIndex(); i < leftChunkValues.size(); i++) {
            sumValue += leftChunkValues.get(i);
        }


        List<Double> rightChunkValues = decompressor.decompress(chunkMap.get(endChunkIndex).array(), metaDataList.get(startChunkIndex).getDataNum());
        for (int i = 0; i < end - metaDataList.get(endChunkIndex).getFirstValueIndex(); i++) {
            sumValue += rightChunkValues.get(i);
        }

        return sumValue;
    }

    public boolean exist(String id, double value) {
        QueryDecompressor decompressor = new QueryDecompressor(new SElfStarDecompressorChunk(new SElfStarXORDecompressor()));
        decompressor.loadMetaData(id);
        return false;
    }

    private boolean isInChunk(int pos, MetaData metaData) {
        return metaData.getFirstValueIndex() <= pos && pos < metaData.getFirstValueIndex() + metaData.getDataNum();
    }
}





















































