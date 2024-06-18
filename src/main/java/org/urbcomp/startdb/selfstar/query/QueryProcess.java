package org.urbcomp.startdb.selfstar.query;

import org.urbcomp.startdb.selfstar.decompressor.SElfStarDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.query.QueryDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.xor.SElfStarXORDecompressor;

import java.util.List;

public class QueryProcess {
    public double max(String id, int start, int end) {
        QueryDecompressor decompressor = new QueryDecompressor(new SElfStarDecompressor(new SElfStarXORDecompressor()));
        decompressor.loadFromFiles(id);
        List<MetaData> metaDataList = decompressor.getMetaDataList();
        List<byte[]> chunkList = decompressor.getChunkList();
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

        if (startChunkIndex == endChunkIndex) {
            List<Double> chunkValues = decompressor.decompress(chunkList.get(startChunkIndex),metaDataList.get(startChunkIndex).getDataNum());
            for (int i = start - metaDataList.get(startChunkIndex).getFirstValueIndex(); i <= end - metaDataList.get(endChunkIndex).getFirstValueIndex(); i++) {
//                System.out.println("a[i] = " + i + " " + chunkValues.get(i));
                maxValue = Math.max(maxValue, chunkValues.get(i));
            }
            return maxValue;
        }

        for (int i = startChunkIndex + 1; i < endChunkIndex; i++) {
            maxValue = Math.max(maxValue, metaDataList.get(i).getMaxValue());
        }
//        System.out.println("maxValue: " + maxValue);
//        System.out.println("chunk idx: " + startChunkIndex + " " + endChunkIndex);
//        System.out.println("first value index: " + metaDataList.get(startChunkIndex).getFirstValueIndex() +
//                " " + metaDataList.get(endChunkIndex).getFirstValueIndex());

        if (maxValue < metaDataList.get(startChunkIndex).getMaxValue()) {
            List<Double> leftChunkValues = decompressor.decompress(chunkList.get(startChunkIndex), metaDataList.get(startChunkIndex).getDataNum());
            for (int i = start - metaDataList.get(startChunkIndex).getFirstValueIndex(); i < leftChunkValues.size(); i++) {
//                System.out.println("a[i] = " + i + " " + leftChunkValues.get(i));
                maxValue = Math.max(maxValue, leftChunkValues.get(i));
            }
        }

        if (maxValue < metaDataList.get(endChunkIndex).getMaxValue()) {
            List<Double> rightChunkValues = decompressor.decompress(chunkList.get(endChunkIndex),metaDataList.get(endChunkIndex).getDataNum());
            for (int i = 0; i <= end - metaDataList.get(endChunkIndex).getFirstValueIndex(); i++) {
//                System.out.println("a[i] = " + i + " " + rightChunkValues.get(i));
                maxValue = Math.max(maxValue, rightChunkValues.get(i));
            }
        }

        return maxValue;
    }

    public double min(int start, int end, List<MetaData> metaDataList, List<byte[]> chunkList) {
        QueryDecompressor decompressor = new QueryDecompressor(new SElfStarDecompressor(new SElfStarXORDecompressor()));
        double minValue = Double.MAX_VALUE;
        int startChunkIndex = -1;
        int endChunkIndex = -1;
        for (MetaData metaData : metaDataList) {
            if (isInChunk(start, metaData)) startChunkIndex = metaData.getFirstValueIndex();
            if (isInChunk(end, metaData)) {
                endChunkIndex = metaData.getFirstValueIndex();
                break;
            }
        }
        if (startChunkIndex == -1 || endChunkIndex == -1) return Double.NaN;
        for (int i = startChunkIndex + 1; i < endChunkIndex; i++) {
            minValue = Math.min(minValue, metaDataList.get(i).getMinValue());
        }

        if (minValue > metaDataList.get(startChunkIndex).getMinValue()) {
            List<Double> leftChunkValues = decompressor.decompress(chunkList.get(startChunkIndex),metaDataList.get(startChunkIndex).getDataNum());
            for (int i = start - metaDataList.get(startChunkIndex).getFirstValueIndex(); i < leftChunkValues.size(); i++) {
                minValue = Math.min(minValue, leftChunkValues.get(i));
            }
        }

        if (minValue > metaDataList.get(startChunkIndex).getMinValue()) {
            List<Double> rightChunkValues = decompressor.decompress(chunkList.get(endChunkIndex),metaDataList.get(startChunkIndex).getDataNum());
            for (int i = 0; i < end - metaDataList.get(endChunkIndex).getFirstValueIndex(); i++) {
                minValue = Math.min(minValue, rightChunkValues.get(i));
            }
        }

        return minValue;
    }

    private boolean isInChunk(int pos, MetaData metaData) {
        return metaData.getFirstValueIndex() <= pos && pos < metaData.getFirstValueIndex() + metaData.getDataNum();
    }
}





















































