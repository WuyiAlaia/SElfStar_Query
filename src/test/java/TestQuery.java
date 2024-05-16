import com.github.Tranway.buff.BuffCompressor;
import org.junit.jupiter.api.Test;
import org.urbcomp.startdb.selfstar.compressor.ICompressor;
import org.urbcomp.startdb.selfstar.compressor.SElfStarCompressor;
import org.urbcomp.startdb.selfstar.compressor.query.BPlusTreeQueryCompressor;
import org.urbcomp.startdb.selfstar.compressor.query.IQueryCompressor;
import org.urbcomp.startdb.selfstar.compressor.query.QueryCompressor;
import org.urbcomp.startdb.selfstar.compressor.xor.SElfXORCompressor;
import org.urbcomp.startdb.selfstar.decompressor.ElfStarDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.IDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.query.BPlusTreeQueryDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.query.IQueryDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.query.QueryDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.xor.SElfStarXORDecompressor;
import org.urbcomp.startdb.selfstar.query.CompressedBlock;
import org.urbcomp.startdb.selfstar.utils.ByteArrayFileReader;

import java.io.*;
import java.util.*;


public class TestQuery {
    private static final String folderPath_Bytes_Tree = "D:/bytes/TreeBytes/";
    private static final String folderPath_Bytes_Chunk = "D:/bytes/ChunkBytes/";
    private static final String RESULT_RandomQueryTIME_CSV = "src/test/resources/result/resultRandomQueryTime.csv";
    private static final String RESULT_MaxQueryTIME_CSV = "src/test/resources/result/resultMaxQueryTime.csv";
    private static final String RESULT_MinQueryTIME_CSV = "src/test/resources/result/resultMinQueryTime.csv";
    private static final String RESULT_RangeQueryTIME_CSV = "src/test/resources/result/resultRangeQueryTime.csv";
    private static final String RESULT_DIFFERENT_POSITION_TIME_CSV = "src/test/resources/result/resultDifferPosTime.csv";
    private static final String RESULT_Bits_CSV = "src/test/resources/result/resultBits.csv";

    private static final double TIME_PRECISION = 1000.0;
    private static final int BLOCK_SIZE = 1000;
    private static final String INIT_FILE = "init.csv";
    private final Map<String, Double> fileNameBaselineToQueryTime = new HashMap<>();
    private final Map<String, Double> fileNameChunkToQueryTime = new HashMap<>();
    private final Map<String, Double> fileNameBTreeToQueryTime = new HashMap<>();
    private final Map<String, Long> fileNameBuffToCompressedBits = new HashMap<>();
    private final Map<String, Long> fileNameSElfToCompressedBits = new HashMap<>();
    private final Map<String, Long> fileNameChunkToCompressedBits = new HashMap<>();
    private final Map<String, Long> fileNameBTreeToCompressedBits = new HashMap<>();
    private final Map<String, List<Double>> fileNameChunkToDifferentPostionInOneBlock = new HashMap<>();
//    private final Map<String, Double> fileNameBaselineToQueryIOTime = new HashMap<>();
//    private final Map<String, Double> fileNameChunkToQueryIOTime = new HashMap<>();
//    private final Map<String, Double> fileNameBaselineToQueryPureTime = new HashMap<>();
//    private final Map<String, Double> fileNameChunkToQueryPureTime = new HashMap<>();
    private final String[] fileNames = {
            "Air-pressure.csv",
            "Air-sensor.csv",
            "Bird-migration.csv",
            "Bitcoin-price.csv",
            "Basel-temp.csv",
            "Basel-wind.csv",
            "City-temp.csv",
            "Dew-point-temp.csv",
            "IR-bio-temp.csv",
            "PM10-dust.csv",
            "Stocks-DE.csv",
            "Stocks-UK.csv",
            "Stocks-USA.csv",
            "Wind-Speed.csv",
            "Blockchain-tr.csv",
            "City-lat.csv",
            "City-lon.csv",
            "Food-price.csv",
            "POI-lat.csv",
            "POI-lon.csv",
            "SSD-bench.csv",
            "electric_vehicle_charging.csv"
    };

    // 测试包括：1. 准确性：随机访问是否正确；最大值最小值查询是否正确；
    //         2. 性能：查询时间比较；块前，块中，块末；压缩率比较


    // 流式

    // 挑战1：无法知道块能压多少数据 -> 退格
    // 挑战2：解压全部花时间
    // 挑战3： 数据在哪一块

    // 先验知识：定长编码和变长编码

    // 查询：随机访问；max/min[a,b];a->index;
    @Test
    public void testRandomQueryIfRight() {
        boolean ifPassOfChunk = false;
        boolean ifPassOfTree = false;
        for (String filename : fileNames) {
            QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename);
            QueryDecompressor qd = new QueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), qc.getBlockFiles());
            BPlusTreeQueryCompressor tqc = new BPlusTreeQueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename);
            BPlusTreeQueryDecompressor tqd = new BPlusTreeQueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), tqc.getBlockFilesTree());

            ifPassOfChunk = randomQueryIfRight(qd, filename);
            ifPassOfTree = randomQueryIfRight(tqd, filename);
        }
        if (ifPassOfChunk) System.out.println("Test of random query in chunk action passed!");
        if (ifPassOfTree) System.out.println("Test of random query in BTree passed!");

    }
    @Test
    public void testMaxMinQueryIfRight() {
        String filename = "Air-pressure.csv";
        QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename);
        QueryDecompressor qd = new QueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), qc.getBlockFiles());
        BPlusTreeQueryCompressor tqc = new BPlusTreeQueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename);
        BPlusTreeQueryDecompressor tqd = new BPlusTreeQueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), tqc.getBlockFilesTree());


        int queryNumber = 100;
        Random random = new Random();
        List<Double> floatings = readfile(filename);
        int numberOfData = floatings.size();
        List<Integer> startList = new ArrayList<>();
        List<Integer> endList = new ArrayList<>();
        for (int i = 0; i < queryNumber; i++) {
            startList.add(random.nextInt(numberOfData));
            endList.add(random.nextInt(numberOfData));
        }

        boolean testMaxMinChunk = true;
        boolean testMaxMinTree = true;
        for (int i = 0 ; i < queryNumber; i++){
            String minChunk = Double.toString(qd.minQuery(startList.get(i), endList.get(i)));
            String minTree = Double.toString(tqd.minQuery(startList.get(i), endList.get(i)));
            String minFromFile = Double.toString(minQueryWithoutCompress(floatings,startList.get(i),endList.get(i)));
            String maxChunk = Double.toString(qd.maxQuery(startList.get(i), endList.get(i)));
            String maxTree = Double.toString(tqd.maxQuery(startList.get(i), endList.get(i)));
            String maxFromFile = Double.toString(maxQueryWithoutCompress(floatings,startList.get(i),endList.get(i)));
            if (!minChunk.equals(minFromFile) || !maxChunk.equals(maxFromFile)) {
                testMaxMinChunk = false;
                System.out.println(startList.get(i) + " " + endList.get(i));
            }
            if (!minTree.equals(minFromFile) || !maxTree.equals(maxFromFile)){
                testMaxMinTree = false;
                if (!minTree.equals(minFromFile)){
                    System.out.println("min:"+startList.get(i) + " " + endList.get(i));
                }
                if (!maxTree.equals(maxFromFile)){
                    System.out.println("max:"+startList.get(i) + " " + endList.get(i));
                }
            }
        }
        if(testMaxMinChunk){
            System.out.println("Test of Max/Min query in chunk action passed!");
        }
        else {
            System.out.println("Test of Max/Min query in chunk action failed!");
        }
        if(testMaxMinTree){
            System.out.println("Test of Max/Min query in chunk+tree action passed!");
        }
        else {
            System.out.println("Test of Max/Min query in chunk+tree action failed!");
        }



    }


    @Test
    public void testRangeQueryIfRight() {
        String filename = "Air-pressure.csv";
        List<Double> floatings = readfile(filename);

        QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename);
        QueryDecompressor qd = new QueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), qc.getBlockFiles());
        BPlusTreeQueryCompressor tqc = new BPlusTreeQueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename);
        BPlusTreeQueryDecompressor tqd = new BPlusTreeQueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), tqc.getBlockFilesTree());


        int testTimes = 100;
        Random random = new Random();
        List<Integer> queryIndexs = new ArrayList<>();
        List<Double> queryFloatings = new ArrayList<>();
        for (int i = 0; i < testTimes; i++){
            int index = random.nextInt(floatings.size());
            queryIndexs.add(index);
            queryFloatings.add(floatings.get(index));
        }

        boolean testRangeChunk = true;
        boolean testRangeTree = true;
        for (double queryFloating : queryFloatings){
            List<Integer> resultChunk = qd.rangeQuery(queryFloating);
            List<Integer> resultTree = tqd.rangeQuery(queryFloating);
            for (Integer r : resultChunk) {
                if (!Double.toString(floatings.get(r)).equals(Double.toString(queryFloating))){
                    testRangeChunk = false;
                    System.out.println(r + queryFloating);
                }
            }
            for (Integer r : resultTree) {
                if (!Double.toString(floatings.get(r)).equals(Double.toString(queryFloating))){
                    testRangeTree = false;

                }
            }
        }

        if (testRangeChunk){
            System.out.println("Test of range query in chunk action passed!");
        }
        else {
            System.out.println("Test of range query in chunk action failed!");
        }
        if (testRangeTree){
            System.out.println("Test of range query in chunk+tree action passed!");
        }
        else {
            System.out.println("Test of range query in chunk+tree action failed!");
        }

    }

    public boolean randomQueryIfRight(IQueryDecompressor qd, String filename) {
        int queryNumber = 10;
        Random random = new Random();
        List<Double> values = readfile(filename);
        int numberOfData = values.size();
        List<Integer> indexList = new ArrayList<>();
        for (int i = 0; i < queryNumber; i++) {
            indexList.add(random.nextInt(numberOfData));
        }


        boolean testRandom = true;
        for (int i = 0; i < indexList.size(); i++) {
            String str1 = Double.toString(qd.randomQuery(indexList.get(i)));
            String str2 = Double.toString(randomQueryWithoutCompress(values, indexList.get(i)));
            if (!str1.equals(str2)) {
                testRandom = false;
                System.out.println(" " + i);
                System.out.println("Test of random query fail!");
                System.out.println("The i is : " + i);
                System.out.println(str1);
                System.out.println(str2);
            }
        }
        return testRandom;
    }

    @Test
    public void testRandomQueryTimeComsumption() throws IOException {

        int queryIndex;
        Random random = new Random();

        Map<String, Double> BaselineRandomQueryTimeForAllDataset = new HashMap<>();
        Map<String, Double> ChunkRandomQueryTimeForAllDataset = new HashMap<>();
        Map<String, Double> BPlusTreeRandomQueryTimeForAllDataset = new HashMap<>();
        for (String filename : fileNames){
            List<Double> floatingsReadFromFile = readfile(filename);
            queryIndex = random.nextInt(floatingsReadFromFile.size());

            //Baseline
            ICompressor compressor = new SElfStarCompressor(new SElfXORCompressor());
            IDecompressor decompressor = new ElfStarDecompressor(new SElfStarXORDecompressor());
            File fileForBaseline = createFiles("SElfCompressor",filename);
            int timesOfWrite = 0;
            try (BlockReader br = new BlockReader(filename, BLOCK_SIZE)) {
                List<Double> floatings;
                while ((floatings = br.nextBlock()) != null) {
                    if (floatings.size() != BLOCK_SIZE) {
                        break;
                    }
                    floatings.forEach(compressor::addValue);
                    compressor.close();
                    writeBytesToFile(compressor.getBytes(),fileForBaseline.getPath());
                    compressor.refresh();
                    timesOfWrite++;
                }
            } catch (Exception e) {
                throw new RuntimeException(filename, e);
            }
            // decompress and query
            double start = System.nanoTime();
            List<Double> allFloatings = new ArrayList<>();
            ByteArrayFileReader byteArrayFileReaderBaseline = new ByteArrayFileReader(fileForBaseline.getPath());
            for (int i = 0; i < timesOfWrite; i++){
                byte[] data = byteArrayFileReaderBaseline.readNextBytes();
                decompressor.setBytes(data);
                List<Double> floatingsOneTime = decompressor.decompress();
                for (Double floating : floatingsOneTime){
                    allFloatings.add(floating);
                }
                decompressor.refresh();
            }
            allFloatings.get(queryIndex);
            System.out.println(allFloatings.size());
            System.out.println(floatingsReadFromFile.size()+"h");
            double baselineRandomQueryTime = (System.nanoTime() - start) / TIME_PRECISION;
            BaselineRandomQueryTimeForAllDataset.put(filename,baselineRandomQueryTime);

            //Chunk
            QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename);
            QueryDecompressor qd = new QueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), qc.getBlockFiles());

            start = System.nanoTime();
            qd.randomQuery(queryIndex);
            double chunkRandomQueryTime = (System.nanoTime() - start) /TIME_PRECISION;
            ChunkRandomQueryTimeForAllDataset.put(filename,chunkRandomQueryTime);

            //BPlusTree + Chunk
            BPlusTreeQueryCompressor tqc = new BPlusTreeQueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename);
            BPlusTreeQueryDecompressor tqd = new BPlusTreeQueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), tqc.getBlockFilesTree());
            start = System.nanoTime();
            tqd.randomQuery(queryIndex);
            double BPlusTreeRandomQueryTime = (System.nanoTime() - start) /TIME_PRECISION;
            BPlusTreeRandomQueryTimeForAllDataset.put(filename,BPlusTreeRandomQueryTime);
        }
        // write result
        try {
            File file = new File(RESULT_RandomQueryTIME_CSV).getParentFile();
            if (!file.exists() && !file.mkdirs()) {
                throw new IOException("Create directory failed: " + file);
            }
            try (FileWriter writer = new FileWriter(RESULT_RandomQueryTIME_CSV, false)) {
                writer.write("Dataset,Baseline, Chunk, Chunk+BTree");
                writer.write("\r\n");
                for (String filename : fileNames) {
                    writer.write(filename);
                    writer.write(",");
                    writer.write(String.format("%.2f", BaselineRandomQueryTimeForAllDataset.get(filename)));
                    writer.write(",");
                    writer.write(String.format("%.2f", ChunkRandomQueryTimeForAllDataset.get(filename)));
                    writer.write(",");
                    writer.write(String.format("%.2f", BPlusTreeRandomQueryTimeForAllDataset.get(filename)));
                    writer.write("\r\n");
                }
                System.out.println("Done!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testMaxQueryTimeComsumption() throws IOException{
        int startIndex;int endIndex;
        Random random = new Random();

        Map<String, Double> BaselineMaxQueryTimeForAllDataset = new HashMap<>();
        Map<String, Double> ChunkMaxQueryTimeForAllDataset = new HashMap<>();
        Map<String, Double> BPlusTreeMaxQueryTimeForAllDataset = new HashMap<>();
        for (String filename : fileNames){
            List<Double> floatingsReadFromFile = readfile(filename);
            startIndex = random.nextInt(floatingsReadFromFile.size());
            endIndex = random.nextInt(floatingsReadFromFile.size());
            if (startIndex > endIndex){
                int tmp = startIndex;
                startIndex = endIndex;
                endIndex = tmp;
            }

            //Baseline
            ICompressor compressor = new SElfStarCompressor(new SElfXORCompressor());
            IDecompressor decompressor = new ElfStarDecompressor(new SElfStarXORDecompressor());
            File fileForBaseline = createFiles("SElfCompressor",filename);
            int timesOfWrite = 0;
            try (BlockReader br = new BlockReader(filename, BLOCK_SIZE)) {
                List<Double> floatings;
                while ((floatings = br.nextBlock()) != null) {
                    if (floatings.size() != BLOCK_SIZE) {
                        break;
                    }
                    floatings.forEach(compressor::addValue);
                    compressor.close();
                    writeBytesToFile(compressor.getBytes(),fileForBaseline.getPath());
                    compressor.refresh();
                    timesOfWrite++;
                }
            } catch (Exception e) {
                throw new RuntimeException(filename, e);
            }
            // decompress and query
            double start = System.nanoTime();
            List<Double> allFloatings = new ArrayList<>();
            ByteArrayFileReader byteArrayFileReaderBaseline = new ByteArrayFileReader(fileForBaseline.getPath());
            for (int i = 0; i < timesOfWrite; i++){
                byte[] data = byteArrayFileReaderBaseline.readNextBytes();
                decompressor.setBytes(data);
                List<Double> floatingsOneTime = decompressor.decompress();
                for (Double floating : floatingsOneTime){
                    allFloatings.add(floating);
                }
                decompressor.refresh();
            }
            double maxBaseline = Double.MIN_VALUE;
            for (int i = startIndex;i <= endIndex;i++){
                if (allFloatings.get(i) > maxBaseline){
                    maxBaseline = allFloatings.get(i);
                }
            }
            double baselineMaxQueryTime = (System.nanoTime() - start) / TIME_PRECISION;
            BaselineMaxQueryTimeForAllDataset.put(filename,baselineMaxQueryTime);

            //Chunk
            QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename);
            QueryDecompressor qd = new QueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), qc.getBlockFiles());

            start = System.nanoTime();
            qd.maxQuery(startIndex,endIndex);
            double chunkMaxQueryTime = (System.nanoTime() - start) /TIME_PRECISION;
            ChunkMaxQueryTimeForAllDataset.put(filename,chunkMaxQueryTime);

            //BPlusTree + Chunk
            BPlusTreeQueryCompressor tqc = new BPlusTreeQueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename);
            BPlusTreeQueryDecompressor tqd = new BPlusTreeQueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), tqc.getBlockFilesTree());
            start = System.nanoTime();
            tqd.maxQuery(startIndex,endIndex);
            double BPlusTreeMaxQueryTime = (System.nanoTime() - start) /TIME_PRECISION;
            BPlusTreeMaxQueryTimeForAllDataset.put(filename,BPlusTreeMaxQueryTime);
        }
        // write result
        try {
            File file = new File(RESULT_MaxQueryTIME_CSV).getParentFile();
            if (!file.exists() && !file.mkdirs()) {
                throw new IOException("Create directory failed: " + file);
            }
            try (FileWriter writer = new FileWriter(RESULT_MaxQueryTIME_CSV, false)) {
                writer.write("Dataset,Baseline, Chunk, Chunk+BTree");
                writer.write("\r\n");
                for (String filename : fileNames) {
                    writer.write(filename);
                    writer.write(",");
                    writer.write(String.format("%.2f", BaselineMaxQueryTimeForAllDataset.get(filename)));
                    writer.write(",");
                    writer.write(String.format("%.2f", ChunkMaxQueryTimeForAllDataset.get(filename)));
                    writer.write(",");
                    writer.write(String.format("%.2f", BPlusTreeMaxQueryTimeForAllDataset.get(filename)));
                    writer.write("\r\n");
                }
                System.out.println("Done!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private File createFiles(String compressor,String datasetFile) {
        File folder = new File("D:/bytes/" + compressor + "/" + datasetFile + "/");
        if (!folder.exists()) {
            folder.mkdirs();
        }
        File file = new File("D:/bytes/" + compressor + "/" + datasetFile + "/" + "compressedBits");
        try {
            if (!file.exists()){
                file.createNewFile();
            }
            else {
                boolean ifClear = clearFile(file);
                if (!ifClear){
                    System.out.println("Fail to clear the file");
                }
            }
        } catch (IOException e) {
            System.out.println("Fail to create the file");
            e.printStackTrace();
        }
        return file;
    }
    private boolean clearFile(File file){
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

//
//    public void testRandomQueryTimeConsumptionForAverage() throws IOException {
//        int times = 1;
//        int queryNumber = 1;
//        double queryRate = 0.01;
//        int blockDataCapacity = 1024;
//        initQueryTimeMap();
//        Map<String, Double> fileNameBaselineToQueryAverageTime = new HashMap<>();
//        Map<String, Double> fileNameChunkToQueryAverageTime = new HashMap<>();
//        Map<String, Double> fileNameBTreeToQueryAverageTime = new HashMap<>();
//        for (int i = 0; i < times; i++) {
//            testRandomQueryTimeConsumption(queryRate,blockDataCapacity);
//        }
//        for (String filename : fileNames) {
//            double BaselineToQueryAllTime = 0;
//            double ChunkToQueryAllTime = 0;
//            double BTreeToQueryAllTime = 0;
//            for (int i = 0; i < times; i++) {
//                BaselineToQueryAllTime += fileNameBaselineToQueryTime.get(filename).get(i);
//                ChunkToQueryAllTime += fileNameChunkToQueryTime.get(filename).get(i);
//                BTreeToQueryAllTime += fileNameBTreeToQueryTime.get(filename).get(i);
//            }
//            fileNameBaselineToQueryAverageTime.put(filename, BaselineToQueryAllTime / times);
//            fileNameChunkToQueryAverageTime.put(filename, ChunkToQueryAllTime / times);
//            fileNameBTreeToQueryAverageTime.put(filename, BTreeToQueryAllTime / times);
//        }
//
//        // write result
//        try {
//            File file = new File(RESULT_TIME_CSV).getParentFile();
//            if (!file.exists() && !file.mkdirs()) {
//                throw new IOException("Create directory failed: " + file);
//            }
//            try (FileWriter writer = new FileWriter(RESULT_TIME_CSV, false)) {
//                writer.write("Dataset,Baseline, Chunk, Chunk+BTree");
//                writer.write("\r\n");
//                for (String filename : fileNames) {
//                    writer.write(filename);
//                    writer.write(",");
//                    writer.write(String.format("%.2f", fileNameBaselineToQueryAverageTime.get(filename)));
//                    writer.write(",");
//                    writer.write(String.format("%.2f", fileNameChunkToQueryAverageTime.get(filename)));
//                    writer.write(",");
//                    writer.write(String.format("%.2f", fileNameBTreeToQueryAverageTime.get(filename)));
//                    writer.write("\r\n");
//                }
//                System.out.println("Done!");
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }


//
//    @Test
//    public void testRandomQueryTimeConsumption() throws IOException {
////        double queryRate = 0.01;
//        int queryNumber = 10;
//        int blockDataCapacity = 1024;
//        Random random = new Random();
//        double start;
//        initQueryTimeMap();
//        for (String filename : fileNames) {
//            List<Double> values = readfile(filename);
//            int numberOfData = values.size();
//            //int queryNumber = (int) Math.ceil(queryRate * numberOfData);
//            List<Integer> indexList = new ArrayList<>();
//            for (int i = 0; i < queryNumber; i++) {
//                indexList.add(random.nextInt(numberOfData));
//            }
//
//            //Baseline:Time of oi + Time of decompress + Time of query
//            double decompressTime = 0;
//            double ioTime = 0;
//            ICompressor compressor = new SElfStarCompressor(new SElfXORCompressor());
//            IDecompressor decompressor = new ElfStarDecompressor(new SElfStarXORDecompressor());
//            List<String> bytesFiles = createFiles(1);
//            refreshFiles(1);
//            int timesOfWrite = 0;
//            try (BlockReader br = new BlockReader(filename, BLOCK_SIZE)) {
//                List<Double> floatings;
//                while ((floatings = br.nextBlock()) != null) {
//                    if (floatings.size() != BLOCK_SIZE) {
//                        break;
//                    }
//                    floatings.forEach(compressor::addValue);
//                    compressor.close();
//                    writeBytesToFile(compressor.getBytes(),bytesFiles.get(0));
//                    compressor.refresh();
//                    timesOfWrite++;
//                }
//            } catch (Exception e) {
//                throw new RuntimeException(filename, e);
//            }
//            ByteArrayFileReader byteArrayFileReaderBaseline = new ByteArrayFileReader(bytesFiles.get(0));
//            for (int i = 0; i < timesOfWrite; i++){
//                start = System.nanoTime();
//                byte[] data = byteArrayFileReaderBaseline.readNextBytes();
//                decompressor.setBytes(data);
//                ioTime += (System.nanoTime() - start) / TIME_PRECISION;
//                start = System.nanoTime();
//                decompressor.decompress();
//                decompressTime += (System.nanoTime() - start) / TIME_PRECISION;
//                decompressor.refresh();
//            }
//
//            start = System.nanoTime();
//            for (int i : indexList) {
//                values.get(i);
//            }
//            double queryBaselineTime = (System.nanoTime() - start) / TIME_PRECISION + decompressTime + ioTime;
//            fileNameBaselineToQueryTime.put(filename,queryBaselineTime);
////            fileNameBaselineToQueryIOTime.put(filename,ioTime);
////            fileNameBaselineToQueryPureTime.put(filename,ioTime);
//
//            //Chunk
//            QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename, blockDataCapacity);
//            QueryDecompressor qd = new QueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), qc.getCompressedBlocks());
//            bytesFiles = createFiles(qc.getCompressedBlocks().size());
//            refreshFiles(qc.getCompressedBlocks().size());
//            for (int i = 0; i < qc.getCompressedBlocks().size(); i++) {
//                writeBytesToFile(qc.getCompressedBlocks().get(i).getData(), bytesFiles.get(i));
//            }
//            ioTime = 0;
//            List<Integer> blocksIndex = randomQueryFindBlockIndex(qd,indexList);
//            for (int i : blocksIndex){
//                ByteArrayFileReader byteArrayFileReaderChunk = new ByteArrayFileReader(bytesFiles.get(i));
//                start = System.nanoTime();
//                byte[] data = byteArrayFileReaderChunk.readNextBytes();
//                qd.setBytes(data);
//                ioTime += (System.nanoTime() - start) / TIME_PRECISION;
//            }
//
//            start = System.nanoTime();
//            randomQueryTimeTest(qd, indexList);
//            double queryChunkTime = (System.nanoTime() - start) / TIME_PRECISION;
//            fileNameChunkToQueryTime.put(filename,queryChunkTime + ioTime);
////            fileNameChunkToQueryIOTime.put(filename,ioTime);
////            fileNameChunkToQueryPureTime.put(filename,ioTime);
//
//            // Chunk + BTree
//            BTreeQueryCompressor btqc = new BTreeQueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename, blockDataCapacity);
//            BTreeQueryDecompressor btqd = new BTreeQueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), btqc.getCompressedBlocksBTree());
//
//            start = System.nanoTime();
//            randomQueryTimeTest(btqd, indexList);
//            double queryBTreeTime = (System.nanoTime() - start) / TIME_PRECISION;
//            fileNameBTreeToQueryTime.put(filename,queryBTreeTime + ioTime);
//        }
//
//        try (FileWriter writer = new FileWriter(RESULT_TIME_CSV, false)) {
//            //writer.write("Dataset,Baseline, Chunk, Chunk+BTree");
//            writer.write("\r\n");
////            for (String filename : fileNames) {
////                writer.write(filename);
////                writer.write(",");
////                writer.write(String.format("%.2f", fileNameBaselineToQueryTime.get(filename)));
////                writer.write(",");
////                writer.write(String.format("%.2f", fileNameChunkToQueryTime.get(filename)));
////                writer.write(",");
////                writer.write(String.format("%.2f", fileNameBTreeToQueryTime.get(filename)));
////                writer.write("\r\n");
////            }
//            writer.write("Baseline");writer.write("\r\n");
//            for (String filename : fileNames) {
//                writer.write(String.format("%.2f", fileNameBaselineToQueryTime.get(filename)));
//                writer.write("\r\n");
//            }
//            writer.write("Chunk");writer.write("\r\n");
//            for (String filename : fileNames) {
//                writer.write(String.format("%.2f", fileNameChunkToQueryTime.get(filename)));
//                writer.write("\r\n");
//            }
//            writer.write("Chunk+BTree");writer.write("\r\n");
//            for (String filename : fileNames) {
//                writer.write(String.format("%.2f", fileNameBTreeToQueryTime.get(filename)));
//                writer.write("\r\n");
//            }
//            System.out.println("Done!");
//        }
//
//    }
//
////    @Test
////    public void testRandomQueryTimeConsumption() throws IOException {
////        int num = 1;
////        int blockDataCapacity = 1024;
////        int queryNumber = num;
////        Random random = new Random();
////        double start;
////        initQueryTimeMap();
////        for (String filename : fileNames) {
////            List<Double> values = readfile(filename);
////            int numberOfData = values.size();
////            List<Integer> indexList = new ArrayList<>();
////            for (int i = 0; i < queryNumber; i++) {
////                indexList.add(random.nextInt(numberOfData));
////            }
////
////            //Baseline:Time of oi + Time of decompress + Time of query
////            double decompressTime = 0;
////            double oiTime = 0;
////            ICompressor compressor = new SElfStarCompressor(new SElfXORCompressor());
////            IDecompressor decompressor = new ElfStarDecompressor(new SElfStarXORDecompressor());
////            List<String> bytesFiles = createFiles(1);
////            refreshFiles(1);
////            int timesOfWrite = 0;
////            try (BlockReader br = new BlockReader(filename, BLOCK_SIZE)) {
////                List<Double> floatings;
////                while ((floatings = br.nextBlock()) != null) {
////                    if (floatings.size() != BLOCK_SIZE) {
////                        break;
////                    }
////                    floatings.forEach(compressor::addValue);
////                    compressor.close();
////                    writeBytesToFile(compressor.getBytes(),bytesFiles.get(0));
////                    compressor.refresh();
////                    timesOfWrite++;
////                }
////            } catch (Exception e) {
////                throw new RuntimeException(filename, e);
////            }
////            ByteArrayFileReader byteArrayFileReaderBaseline = new ByteArrayFileReader(bytesFiles.get(0));
////            for (int i = 0; i < timesOfWrite; i++){
////                start = System.nanoTime();
////                byte[] data = byteArrayFileReaderBaseline.readNextBytes();
////                decompressor.setBytes(data);
////                oiTime += (System.nanoTime() - start) / TIME_PRECISION;
////                start = System.nanoTime();
////                decompressor.decompress();
////                decompressTime += (System.nanoTime() - start) / TIME_PRECISION;
////                decompressor.refresh();
////            }
////
////            start = System.nanoTime();
////            for (int i : indexList) {
////                values.get(i);
////            }
////            double queryBaselineTime = (System.nanoTime() - start) / TIME_PRECISION + decompressTime + oiTime;
////            fileNameBaselineToQueryTime.put(filename,queryBaselineTime);
////
////            //Chunk
////            QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename, blockDataCapacity);
////            QueryDecompressor qd = new QueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), qc.getCompressedBlocks());
////            bytesFiles = createFiles(qc.getCompressedBlocks().size());
////            refreshFiles(qc.getCompressedBlocks().size());
////            for (int i = 0; i < qc.getCompressedBlocks().size(); i++) {
////                writeBytesToFile(qc.getCompressedBlocks().get(i).getData(), bytesFiles.get(i));
////            }
////            oiTime = 0;
////            List<Integer> blocksIndex = randomQueryFindBlockIndex(qd,indexList);
////            for (int i : blocksIndex){
////                ByteArrayFileReader byteArrayFileReaderChunk = new ByteArrayFileReader(bytesFiles.get(i));
////                start = System.nanoTime();
////                byte[] data = byteArrayFileReaderChunk.readNextBytes();
////                qd.setBytes(data);
////                oiTime += (System.nanoTime() - start) / TIME_PRECISION;
////            }
////
////            start = System.nanoTime();
////            randomQueryTimeTest(qd, indexList);
////            double queryChunkTime = (System.nanoTime() - start) / TIME_PRECISION;
////            fileNameChunkToQueryTime.put(filename,queryChunkTime + oiTime);
////
////            // Chunk + BTree
////            BTreeQueryCompressor btqc = new BTreeQueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename, blockDataCapacity);
////            BTreeQueryDecompressor btqd = new BTreeQueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), btqc.getCompressedBlocksBTree());
////
////            start = System.nanoTime();
////            randomQueryTimeTest(btqd, indexList);
////            double queryBTreeTime = (System.nanoTime() - start) / TIME_PRECISION;
////            fileNameBTreeToQueryTime.put(filename,queryBTreeTime + oiTime);
////        }
////    }
//
//    public void testRandomQueryTimeConsumptionInOneBlock(double rate,int blockDataCapacity) throws IOException {
//
//        double queryRate = rate;
//        Random random = new Random();
//        double start;
//        for (String filename : fileNames) {
//            List<Double> values = readfile(filename);
//            int numberOfData = values.size();
//            int queryNumber = (int) Math.ceil(queryRate * numberOfData);
//            List<Integer> indexList = new ArrayList<>();
//
//            ICompressor compressor = new SElfStarCompressor(new SElfXORCompressor());
//            IDecompressor decompressor = new ElfStarDecompressor(new SElfStarXORDecompressor());
//            QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename, blockDataCapacity);
//            QueryDecompressor qd = new QueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), qc.getCompressedBlocks());
//            BTreeQueryCompressor btqc = new BTreeQueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename, blockDataCapacity);
//            BTreeQueryDecompressor btqd = new BTreeQueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), btqc.getCompressedBlocksBTree());
//
//
//            for (int i = 0; i < queryNumber; i++) {
//                indexList.add(random.nextInt(qc.getCompressedBlocks().get(0).getDataNumber()));
//            }
//
//            //Baseline:Time of oi + Time of decompress + Time of query
//            double decompressTime = 0;
//            double oiTime = 0;
//            List<String> bytesFiles = createFiles(1);
//            refreshFiles(1);
//            int timesOfWrite = 0;
//            try (BlockReader br = new BlockReader(filename, BLOCK_SIZE)) {
//                List<Double> floatings;
//                while ((floatings = br.nextBlock()) != null) {
//                    if (floatings.size() != BLOCK_SIZE) {
//                        break;
//                    }
//                    floatings.forEach(compressor::addValue);
//                    compressor.close();
//                    writeBytesToFile(compressor.getBytes(),bytesFiles.get(0));
//                    compressor.refresh();
//                    timesOfWrite++;
//                }
//            } catch (Exception e) {
//                throw new RuntimeException(filename, e);
//            }
//            ByteArrayFileReader byteArrayFileReaderBaseline = new ByteArrayFileReader(bytesFiles.get(0));
//            for (int i = 0; i < timesOfWrite; i++){
//                start = System.nanoTime();
//                byte[] data = byteArrayFileReaderBaseline.readNextBytes();
//                decompressor.setBytes(data);
//                oiTime += (System.nanoTime() - start) / TIME_PRECISION;
//                start = System.nanoTime();
//                decompressor.decompress();
//                decompressTime += (System.nanoTime() - start) / TIME_PRECISION;
//                decompressor.refresh();
//            }
//
//            start = System.nanoTime();
//            for (int i : indexList) {
//                values.get(i);
//            }
//            double queryBaselineTime = (System.nanoTime() - start) / TIME_PRECISION + decompressTime + oiTime;
//            fileNameBaselineToQueryTime.put(filename,queryBaselineTime);
//
//            //Chunk
//            bytesFiles = createFiles(qc.getCompressedBlocks().size());
//            refreshFiles(qc.getCompressedBlocks().size());
//            for (int i = 0; i < qc.getCompressedBlocks().size(); i++) {
//                writeBytesToFile(qc.getCompressedBlocks().get(i).getData(), bytesFiles.get(i));
//            }
//            oiTime = 0;
//            List<Integer> blocksIndex = randomQueryFindBlockIndex(qd,indexList);
//            for (int i : blocksIndex){
//                ByteArrayFileReader byteArrayFileReaderChunk = new ByteArrayFileReader(bytesFiles.get(i));
//                start = System.nanoTime();
//                byte[] data = byteArrayFileReaderChunk.readNextBytes();
//                qd.setBytes(data);
//                oiTime += (System.nanoTime() - start) / TIME_PRECISION;
//            }
//
//            start = System.nanoTime();
//            randomQueryTimeTest(qd, indexList);
//            double queryChunkTime = (System.nanoTime() - start) / TIME_PRECISION;
//            fileNameChunkToQueryTime.put(filename,queryChunkTime + oiTime);
//
//            // Chunk + BTree
//            start = System.nanoTime();
//            randomQueryTimeTest(btqd, indexList);
//            double queryBTreeTime = (System.nanoTime() - start) / TIME_PRECISION;
//            fileNameBTreeToQueryTime.put(filename,queryBTreeTime + oiTime);
//        }
//
//    }
//
//
//    public void testRandomQueryTimeConsumptionInOneBlock(int num,int blockDataCapacity) throws IOException {
//        int queryNumber = num;
//        Random random = new Random();
//        double start;
//        for (String filename : fileNames) {
//            List<Double> values = readfile(filename);
//            int numberOfData = values.size();
//            List<Integer> indexList = new ArrayList<>();
//
//            ICompressor compressor = new SElfStarCompressor(new SElfXORCompressor());
//            IDecompressor decompressor = new ElfStarDecompressor(new SElfStarXORDecompressor());
//            QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename, blockDataCapacity);
//            QueryDecompressor qd = new QueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), qc.getCompressedBlocks());
//            BTreeQueryCompressor btqc = new BTreeQueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename, blockDataCapacity);
//            BTreeQueryDecompressor btqd = new BTreeQueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), btqc.getCompressedBlocksBTree());
//
//            for (int i = 0; i < queryNumber; i++) {
//                indexList.add(random.nextInt(qc.getCompressedBlocks().get(0).getDataNumber()));
//            }
//
//            //Baseline:Time of oi + Time of decompress + Time of query
//            double decompressTime = 0;
//            double oiTime = 0;
//            List<String> bytesFiles = createFiles(1);
//            refreshFiles(1);
//            int timesOfWrite = 0;
//            try (BlockReader br = new BlockReader(filename, BLOCK_SIZE)) {
//                List<Double> floatings;
//                while ((floatings = br.nextBlock()) != null) {
//                    if (floatings.size() != BLOCK_SIZE) {
//                        break;
//                    }
//                    floatings.forEach(compressor::addValue);
//                    compressor.close();
//                    writeBytesToFile(compressor.getBytes(),bytesFiles.get(0));
//                    compressor.refresh();
//                    timesOfWrite++;
//                }
//            } catch (Exception e) {
//                throw new RuntimeException(filename, e);
//            }
//            ByteArrayFileReader byteArrayFileReaderBaseline = new ByteArrayFileReader(bytesFiles.get(0));
//            for (int i = 0; i < timesOfWrite; i++){
//                start = System.nanoTime();
//                byte[] data = byteArrayFileReaderBaseline.readNextBytes();
//                decompressor.setBytes(data);
//                oiTime += (System.nanoTime() - start) / TIME_PRECISION;
//                start = System.nanoTime();
//                decompressor.decompress();
//                decompressTime += (System.nanoTime() - start) / TIME_PRECISION;
//                decompressor.refresh();
//            }
//
//            start = System.nanoTime();
//            for (int i : indexList) {
//                values.get(i);
//            }
//            double queryBaselineTime = (System.nanoTime() - start) / TIME_PRECISION + decompressTime + oiTime;
//            fileNameBaselineToQueryTime.put(filename,queryBaselineTime);
//
//            //Chunk
//            bytesFiles = createFiles(qc.getCompressedBlocks().size());
//            refreshFiles(qc.getCompressedBlocks().size());
//            for (int i = 0; i < qc.getCompressedBlocks().size(); i++) {
//                writeBytesToFile(qc.getCompressedBlocks().get(i).getData(), bytesFiles.get(i));
//            }
//            oiTime = 0;
//            List<Integer> blocksIndex = randomQueryFindBlockIndex(qd,indexList);
//            for (int i : blocksIndex){
//                ByteArrayFileReader byteArrayFileReaderChunk = new ByteArrayFileReader(bytesFiles.get(i));
//                start = System.nanoTime();
//                byte[] data = byteArrayFileReaderChunk.readNextBytes();
//                qd.setBytes(data);
//                oiTime += (System.nanoTime() - start) / TIME_PRECISION;
//            }
//
//            start = System.nanoTime();
//            randomQueryTimeTest(qd, indexList);
//            double queryChunkTime = (System.nanoTime() - start) / TIME_PRECISION;
//            fileNameChunkToQueryTime.put(filename,queryChunkTime + oiTime);
//
//            // Chunk + BTree
//            start = System.nanoTime();
//            randomQueryTimeTest(btqd, indexList);
//            double queryBTreeTime = (System.nanoTime() - start) / TIME_PRECISION;
//            fileNameBTreeToQueryTime.put(filename,queryBTreeTime + oiTime);
//        }
//    }
//
//    @Test
//    public void testCompressionSpace() {
//        int blockDataCapacity = 1024*16;
//        int blockSize = 1000;
//        for (String filename : fileNames) {
//            // Baseline: Buff
//            long compressedBitsBuff = 0;
//            try (BlockReader br = new BlockReader(filename, blockSize)) {
//                List<Double> floatings;
//                while ((floatings = br.nextBlock()) != null) {
//                    if (floatings.size() != blockSize) {
//                        break;
//                    }
//                    double[] values = floatings.stream()
//                            .mapToDouble(Double::doubleValue)
//                            .toArray();
//                    BuffCompressor compressor = new BuffCompressor(blockSize);
//                    compressor.compress(values);
//                    compressedBitsBuff += compressor.getSize();
//                }
//                fileNameBuffToCompressedBits.put(filename, compressedBitsBuff);
//            } catch (Exception e) {
//                throw new RuntimeException(filename, e);
//            }
//
//            //Baseline: SElf
//            long compressedBitsSElf = 0;
//            ICompressor SElfCompressor = new SElfStarCompressor(new SElfXORCompressor());
//            try (BlockReader br = new BlockReader(filename, blockSize)) {
//                List<Double> floatings;
//                while ((floatings = br.nextBlock()) != null) {
//                    if (floatings.size() != blockSize) {
//                        break;
//                    }
//                    SElfCompressor.refresh();
//                    floatings.forEach(SElfCompressor::addValue);
//                    compressedBitsSElf += SElfCompressor.getCompressedSizeInBits();
//                }
//                fileNameSElfToCompressedBits.put(filename, compressedBitsSElf);
//            } catch (Exception e) {
//                throw new RuntimeException(filename, e);
//            }
//
//            // Chunk
//            QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename, blockDataCapacity);
//            // as for one CompressedBlock = ( int * 3 + double * 2 + long * 1 ) + blockDataCapacity
//            int compressedBlockBits = (4 * 3 + 8 * 3) * 8 + blockDataCapacity;
//            long compressedBitsChunk = compressedBlockBits * qc.getCompressedBlocks().size();
//            fileNameChunkToCompressedBits.put(filename, compressedBitsChunk);
//
//            //Chunk+BTree
//            BTreeQueryCompressor btqc = new BTreeQueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename, blockDataCapacity);
//            // as for one BTreeNode = (reference * 2 + boolean * 1 + int * 1)
//            long BTreeBits = (8 * 2 + 1 + 4) * 8 * btqc.getCompressedBlocksBTree().getTreeNodeNumber();
//            fileNameBTreeToCompressedBits.put(filename, compressedBitsChunk + BTreeBits);
//
//        }
//
//        // write result
//        try {
//            File file = new File(RESULT_Bits_CSV).getParentFile();
//            if (!file.exists() && !file.mkdirs()) {
//                throw new IOException("Create directory failed: " + file);
//            }
//            try (FileWriter writer = new FileWriter(RESULT_Bits_CSV, false)) {
//                writer.write("Dataset,Buff, SElf, Chunk, Chunk+BTree");
//                writer.write("\r\n");
//                for (String filename : fileNames) {
//                    writer.write(filename);
//                    writer.write(",");
//                    writer.write(fileNameBuffToCompressedBits.get(filename).toString());
//                    writer.write(",");
//                    writer.write(fileNameSElfToCompressedBits.get(filename).toString());
//                    writer.write(",");
//                    writer.write(fileNameChunkToCompressedBits.get(filename).toString());
//                    writer.write(",");
//                    writer.write(fileNameBTreeToCompressedBits.get(filename).toString());
//                    writer.write("\r\n");
//                }
//                System.out.println("Done!");
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Test
//    public void testDifferentPostionInOneBlock(){
//        int blockDataCapacity = 2048;
//        for (String filename : fileNames){
//            QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename, blockDataCapacity);
//            QueryDecompressor qd = new QueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), qc.getCompressedBlocks());
//            int firstBlockDataNumber = qc.getCompressedBlocks().get(0).getDataNumber();
//            double start = System.nanoTime();
//            qd.RandomQuery(firstBlockDataNumber * 0);
//            double end = System.nanoTime();
//            double queryTime = (end - start) / TIME_PRECISION;
//            System.out.println(queryTime);
////            List<Integer> listIndexs = new ArrayList<>();
////            listIndexs.add(0);
////            listIndexs.add(firstBlockDataNumber * 1/4);
////            listIndexs.add(firstBlockDataNumber * 1/2);
////            listIndexs.add(firstBlockDataNumber * 3/4);
////            listIndexs.add(firstBlockDataNumber - 1);
////            double start;
////            double queryTime;
////            fileNameChunkToDifferentPostionInOneBlock.put(filename,new ArrayList<>());
////            for (int i : listIndexs){
////                start = System.nanoTime();
////                qd.RandomQuery(i);
////                queryTime = (System.nanoTime() - start ) /TIME_PRECISION;
////                qd.refresh();
////                fileNameChunkToDifferentPostionInOneBlock.get(filename).add(queryTime);
////            }
//        }
//
////        try (FileWriter writer = new FileWriter(RESULT_DIFFERENT_POSITION_TIME_CSV, false)) {
////            writer.write("Dataset,0, 1/4, 1/2, 3/4, 1");
////            writer.write("\r\n");
////            for (String filename : fileNames) {
////                writer.write(filename);writer.write(",");
////                for (int i = 0; i < 5; i++){
////                    writer.write(String.format("%.2f", fileNameChunkToDifferentPostionInOneBlock.get(filename).get(i)));
////                    writer.write(",");
////                }
////                writer.write("\r\n");
////            }
////            System.out.println("Done!");
////
////        }catch (IOException e) {
////            throw new RuntimeException(e);
////        }
//    }
//
//        @Test
//    public void testBasicQuery() {
//        String filename = "Air-pressure.csv";
//        QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename);
//        QueryDecompressor qd = new QueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), qc.getCompressedBlocks());
//
//        for (int i=0 ;i<qc.getCompressedBlocks().size();i++){
//            System.out.println(qc.getCompressedBlocks().get(i).getIData());
//        }
//
//        List<Double> floatings = readfile(filename);
//
//
//        boolean testRandom = true;
//        for (int i = 0; i < floatings.size(); i++) {
//            String str1 = Double.toString(qd.RandomQuery(i));
//            String str2 = Double.toString(randomQueryWithoutCompress(floatings, i));
//            if (!str1.equals(str2)) {
//                testRandom = false;
//                System.out.println(" " + i);
//                System.out.println("Test of random query fail!");
//                System.out.println("The i is : " + i);
//                System.out.println(str1);
//                System.out.println(str2);
//            }
//        }
//        System.out.println();
//        if (testRandom) {
//            System.out.println("Test of random query pass!");
//        }
//
////        int i = 365;
////        int start = 24;
////        int end = 44;
////
////        System.out.println(i+"-th data:"+qd.RandomQuery(i));
////        System.out.println(Long.toBinaryString(Double.doubleToLongBits(qd.RandomQuery(i))) + " " +Double.doubleToLongBits(qd.RandomQuery(i)));
////        System.out.println(i+"-th data:"+randomQueryWithoutCompress(floatings,i));
////        System.out.println(Long.toBinaryString(Double.doubleToLongBits(randomQueryWithoutCompress(floatings,i))) + " " + Double.doubleToLongBits(randomQueryWithoutCompress(floatings,i)));
////        System.out.println();
//
////        String str1 = Double.toString(qd.RandomQuery(i));
////        String str2 = Double.toString(randomQueryWithoutCompress(floatings,i));
////        if (!str1.equals(str2)){
////            System.out.println("random query error");
////            System.out.println(qd.RandomQuery(i));
////            System.out.println(randomQueryWithoutCompress(floatings,i));
////        }
//
////        System.out.println("max[start,end]:"+qd.MaxQuery(start,end));
////        System.out.println("max[start,end]:"+maxQueryWithoutCompress(floatings,start,end));
////        System.out.println();
////
////        System.out.println("min[start,end]:"+qd.MinQuery(start,end));
////        System.out.println("min[start,end]:"+minQueryWithoutCompress(floatings,start,end));
////        System.out.println();
//
//        System.out.println("Test Complete!");
//        System.gc();
//
//
//    }
//
//    @Test
//    public void TestTreeQuery() {
//        String filename = "Basel-temp.csv";
//        BTreeQueryCompressor btqc = new BTreeQueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), filename);
//        BTreeQueryDecompressor btqd = new BTreeQueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), btqc.getCompressedBlocksBTree());
//
//        List<Double> floatings = readfile(filename);
//
//        boolean testRandom = true;
//        for (int i = 0; i < floatings.size(); i++) {
//            String str1 = Double.toString(btqd.RandomQuery(i));
//            String str2 = Double.toString(randomQueryWithoutCompress(floatings, i));
//            if (!str1.equals(str2)) {
//                testRandom = false;
//                System.out.println(" " + i);
//                System.out.println("Test of random query fail!");
//                System.out.println("The i is : " + i);
//                System.out.println(str1);
//                System.out.println(str2);
//            }
//        }
//        System.out.println();
//        if (testRandom) {
//            System.out.println("Test of random query pass!");
//        }
//
////        int i = 2500;
////        int start = 24;
////        int end = 26;
////
////        System.out.println(i+"-th data:"+btqd.RandomQuery(i));
////        System.out.println(i+"-th data:"+randomQueryWithoutCompress(floatings,i));
////        if (!String.valueOf(btqd.RandomQuery(i)).equals(String.valueOf(randomQueryWithoutCompress(floatings,i)))){
////            System.out.println("random query error");
////        }
////        System.out.println();
////
////        System.out.println("max[start,end]:"+btqd.MaxQuery(start,end));
////        System.out.println("max[start,end]:"+maxQueryWithoutCompress(floatings,start,end));
////        System.out.println();
////
////        System.out.println("min[start,end]:"+btqd.MinQuery(start,end));
////        System.out.println("min[start,end]:"+minQueryWithoutCompress(floatings,start,end));
////        System.out.println();
//
//        System.out.println("Test Complete!");
//        System.gc();
//    }
//

//
//    public void randomQueryTimeTest(IQueryDecompressor qd, List<Integer> indexListForQuery) {
//        for (int i : indexListForQuery) {
//            qd.RandomQuery(i);
//        }
//    }
//
//    public List<Integer> randomQueryFindBlockIndex (IQueryDecompressor qd, List<Integer> indexListForQuery){
//        List<Integer> blocksIndex = new ArrayList<>();
//        for (int i: indexListForQuery){
//            blocksIndex.add(qd.randomQueryFindBlock(i));
//        }
//        return blocksIndex;
//    }
//
//
    public List<Double> readfile(String filename) {
        List<Double> floatings = new ArrayList<>();
        try (BlockReader br = new BlockReader(filename, 1000)) {
            List<Double> tmp;
            while ((tmp = br.nextBlock()) != null) {
                floatings.addAll(tmp);
            }
        } catch (Exception e) {
            throw new RuntimeException(filename, e);
        }
        return floatings;
    }
    public Double randomQueryWithoutCompress(List<Double> floatings, int i) {
        return floatings.get(i);
    }

    public Double minQueryWithoutCompress(List<Double> floatings, int s, int e) {
        if (s == e){
            return floatings.get(s);
        }
        if (s > e){
            int tmp = s;
            s = e;
            e = tmp;
        }

        Double min = Double.MAX_VALUE;
        for (int i = s; i <= e; i++) {
            if (floatings.get(i) < min) {
                min = floatings.get(i);
            }
        }
        return min;
    }
    public Double maxQueryWithoutCompress(List<Double> floatings, int s, int e) {
        if (s == e){
            return floatings.get(s);
        }
        if (s > e){
            int tmp = s;
            s = e;
            e = tmp;
        }

        Double max = Double.MIN_VALUE;
        for (int i = s; i <= e; i++) {
            if (floatings.get(i) > max) {
                max = floatings.get(i);
            }
        }
        return max;
    }
//
//
//    @Test
//    public void testFile() throws IOException {
//        byte[] bytes = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
//        refreshFiles(1);
//        writeBytesToFile(bytes, folderPath_Bytes + "fileBytes_1.txt");
//        byte[] bytes2 = {0, 1, 2, 3};
//        writeBytesToFile(bytes2, folderPath_Bytes + "fileBytes_1.txt");
//        ByteArrayFileReader byteArrayFileReader = new ByteArrayFileReader(folderPath_Bytes + "fileBytes_1.txt");
//
//        byte[] newB = byteArrayFileReader.readNextBytes();
//        byte[] newX = byteArrayFileReader.readNextBytes();
//        for (byte b : newB) {
//            System.out.println(b);
//        }
//        for (byte b : newX) {
//            System.out.println(b);
//        }
//    }
//
//    // 将压缩块写入文件
    public void writeBytesToFile(byte[] data, String filePath) throws IOException {
        FileOutputStream fos = new FileOutputStream(filePath, true); // 追加模式
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        DataOutputStream dos = new DataOutputStream(bos);

        // 写入数据长度
        dos.writeInt(data.length);

        // 写入数据本身
        bos.write(data);

        dos.flush();
        dos.close();
    }
//
//    private void initQueryTimeMap() {
//        for (String filename : fileNames) {
//            fileNameBaselineToQueryTime.put(filename, 0.0);
//            fileNameChunkToQueryTime.put(filename, 0.0);
//            fileNameBTreeToQueryTime.put(filename, 0.0);
//        }
//    }
//
//    private List<String> createFiles(int fileNumber) {
//        File folder = new File(folderPath_Bytes);
//        List<String> bytesFiles = new ArrayList<>();
//        if (!folder.exists()) {
//            folder.mkdirs();
//        }
//
//        for (int i = 1; i <= fileNumber; i++) {
//            String fileName = "fileBytes_" + i + ".txt";
//            File file = new File(folderPath_Bytes + fileName);
//            try {
//                if (file.exists()) {
//                    bytesFiles.add(folderPath_Bytes + fileName);
//                } else {
//                    if (file.createNewFile()) {
//                        bytesFiles.add(fileName);
//                    } else {
//                        System.out.println("Fail to create the file: " + fileName);
//                    }
//                }
//            } catch (IOException e) {
//                System.out.println("Fail to create the file:" + fileName);
//                e.printStackTrace();
//            }
//        }
//        return bytesFiles;
//
//    }
//
//    private void refreshFiles(int fileNumber) {
//        for (int i = 1; i <= fileNumber; i++) {
//            String fileName = "fileBytes_" + i + ".txt";
//            File file = new File(folderPath_Bytes + fileName);
//            try {
//                // 使用 FileWriter 打开文件，并传入 true 参数表示追加写入
//                FileWriter writer = new FileWriter(file, false);
//                // 将文件内容清空
//                writer.write("");
//                // 关闭文件写入流
//                writer.close();
//            } catch (IOException e) {
//                System.out.println("文件内容清空失败：" + fileName);
//                e.printStackTrace();
//            }
//        }
//    }
//    private void deleteFiles(int fileNumber) {
//        for (int i = 1; i <= fileNumber; i++) {
//            String fileName = "fileBytes_" + i + ".txt";
//            File file = new File(folderPath_Bytes + fileName);
//            try {
//                // 使用 FileWriter 打开文件，并传入 true 参数表示追加写入
//                FileWriter writer = new FileWriter(file, false);
//                // 将文件内容清空
//                writer.write("");
//                // 关闭文件写入流
//                writer.close();
//            } catch (IOException e) {
//                System.out.println("文件内容清空失败：" + fileName);
//                e.printStackTrace();
//            }
//        }
//    }
//

}

