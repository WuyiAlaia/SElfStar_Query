import com.github.Tranway.buff.BuffCompressor;
import org.junit.jupiter.api.Test;
import org.urbcomp.startdb.selfstar.compressor.ICompressor;
import org.urbcomp.startdb.selfstar.compressor.SElfStarCompressor;
import org.urbcomp.startdb.selfstar.compressor.query.BTreeQueryCompressor;
import org.urbcomp.startdb.selfstar.compressor.query.QueryCompressor;
import org.urbcomp.startdb.selfstar.compressor.xor.SElfXORCompressor;
import org.urbcomp.startdb.selfstar.decompressor.ElfStarDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.IDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.query.BTreeQueryDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.query.IQueryDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.query.QueryDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.xor.SElfStarXORDecompressor;


import java.io.*;
import java.util.*;


public class TestQuery {

    private static final String RESULT_TIME_CSV = "src/test/resources/result/resultTime.csv";
    private static final String RESULT_Bits_CSV = "src/test/resources/result/resultBits.csv";
    private  static final String folderPath_Bytes = "src/test/resources/bytes/";

    private static final double TIME_PRECISION = 1000.0;
    private static final int BLOCK_SIZE = 1000;
    private static final String INIT_FILE = "init.csv";
    private final Map<String,List<Double>> fileNameBaselineToQueryTime = new HashMap<>();
    private final Map<String, List<Double>> fileNameChunkToQueryTime = new HashMap<>();
    private final Map<String, List<Double>> fileNameBTreeToQueryTime = new HashMap<>();
    private final Map<String, Long> fileNameBuffToCompressedBits = new HashMap<>();
    private final Map<String, Long> fileNameSElfToCompressedBits = new HashMap<>();
    private final Map<String, Long> fileNameChunkToCompressedBits = new HashMap<>();
    private final Map<String, Long> fileNameBTreeToCompressedBits = new HashMap<>();
    private final String[] fileNames = {
            INIT_FILE,
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
    public void testRandomQueryIfRight(){
        boolean ifPassOfChunk = false;
        boolean ifPassOfTree = false;
        for (String filename : fileNames){
            QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()),filename);
            QueryDecompressor qd = new QueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), qc.getCompressedBlocks());

            BTreeQueryCompressor btqc = new BTreeQueryCompressor(new SElfStarCompressor(new SElfXORCompressor()),filename);
            BTreeQueryDecompressor btqd = new BTreeQueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), btqc.getCompressedBlocksBTree());

            ifPassOfChunk = randomQueryIfRight(qd,filename);
            ifPassOfTree = randomQueryIfRight(btqd,filename);
        }
        if(ifPassOfChunk) System.out.println("Test of random query in chunk action passed!");
        if(ifPassOfTree) System.out.println("Test of random query in BTree passed!");

    }

    @Test
    public void testRandomQueryTimeConsumptionForAverage(){
        int times = 2;
        int queryNumber = 100;
        double queryRate = 0.0001;
        initQueryTimeMap();
        Map<String, Double> fileNameBaselineToQueryAverageTime = new HashMap<>();
        Map<String, Double> fileNameChunkToQueryAverageTime = new HashMap<>();
        Map<String, Double> fileNameBTreeToQueryAverageTime = new HashMap<>();
        for (int i=0; i<times; i++){
            testRandomQueryTimeConsumption(queryRate);
        }
        for (String filename : fileNames){
            double BaselineToQueryAllTime = 0;
            double ChunkToQueryAllTime = 0;
            double BTreeToQueryAllTime = 0;
            for (int i=0; i < times; i++){
                BaselineToQueryAllTime += fileNameBaselineToQueryTime.get(filename).get(i);
                ChunkToQueryAllTime += fileNameChunkToQueryTime.get(filename).get(i);
                BTreeToQueryAllTime += fileNameBTreeToQueryTime.get(filename).get(i);
            }
            fileNameBaselineToQueryAverageTime.put(filename,BaselineToQueryAllTime/times);
            fileNameChunkToQueryAverageTime.put(filename,ChunkToQueryAllTime/times);
            fileNameBTreeToQueryAverageTime.put(filename,BTreeToQueryAllTime/times);
        }

        // write result
        try {
            File file = new File(RESULT_TIME_CSV).getParentFile();
            if (!file.exists() && !file.mkdirs()) {
                throw new IOException("Create directory failed: " + file);
            }
            try (FileWriter writer = new FileWriter(RESULT_TIME_CSV, false)) {
                writer.write("Dataset,Baseline, Chunk, Chunk+BTree");
                writer.write("\r\n");
                for (String filename : fileNames) {
                    writer.write(filename);
                    writer.write(",");
                    writer.write(String.format("%.2f", fileNameBaselineToQueryAverageTime.get(filename)));
                    writer.write(",");
                    writer.write(String.format("%.2f", fileNameChunkToQueryAverageTime.get(filename)));
                    writer.write(",");
                    writer.write(String.format("%.2f",fileNameBTreeToQueryAverageTime.get(filename)));
                    writer.write("\r\n");
                }
                System.out.println("Done!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRangeQuery(){
        double queryFloating = 88.51872;
        String filename = "Air-pressure.csv";

        QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()),filename);
        QueryDecompressor qd = new QueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), qc.getCompressedBlocks());

        BTreeQueryCompressor btqc = new BTreeQueryCompressor(new SElfStarCompressor(new SElfXORCompressor()),filename);
        BTreeQueryDecompressor btqd = new BTreeQueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), btqc.getCompressedBlocksBTree());

        List<Integer> resultChunk = qd.RangeQuery(queryFloating);
        List<Integer> resultBTree = btqd.RangeQuery(queryFloating);
        for (Integer r : resultChunk){
            System.out.println(r);
        }
    }

    public void testRandomQueryTimeConsumption(double rate){
//        List<String> bytesFiles = createFiles(100);

        double queryRate = rate;
        Random random = new Random();
        double start;
        for (String filename : fileNames){
            List<Double> values = readfile(filename);
            int numberOfData = values.size();
            int queryNumber = (int) Math.ceil(queryRate * numberOfData);
            List<Integer> indexList = new ArrayList<>();
            for (int i=0; i < queryNumber; i++){
                indexList.add(random.nextInt(numberOfData));
            }

            //Baseline:Time of oi + Time of decompress + Time of query
            double decompressTime = 0;
//            double oiTime = 0;
            ICompressor compressor = new SElfStarCompressor(new SElfXORCompressor());
            IDecompressor decompressor = new ElfStarDecompressor(new SElfStarXORDecompressor());
            try (BlockReader br = new BlockReader(filename, BLOCK_SIZE)) {
                List<Double> floatings;
                while ((floatings = br.nextBlock()) != null) {
                    if (floatings.size() != BLOCK_SIZE) {
                        break;
                    }
                    floatings.forEach(compressor::addValue);
                    compressor.close();
//                    refreshFiles(1);
//                    writeBytesToFile(compressor.getBytes(),bytesFiles.get(0));
//                    start = System.nanoTime();
//                    decompressor.setBytes(readBytesFromFile(bytesFiles.get(0)));
//                    oiTime += (System.nanoTime() - start) / TIME_PRECISION;

                    start = System.nanoTime();
                    List<Double> devalues = decompressor.decompress();
                    decompressTime += (System.nanoTime() - start) / TIME_PRECISION;
                    compressor.refresh();
                    decompressor.refresh();
                }
            } catch (Exception e) {
                throw new RuntimeException(filename, e);
            }
            start = System.nanoTime();
            for (int i:indexList){
                values.get(i);
            }
            double queryBaselineTime = (System.nanoTime() - start) / TIME_PRECISION + decompressTime;
            fileNameBaselineToQueryTime.get(filename).add(queryBaselineTime);

            //Chunk
            QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()),filename);
            QueryDecompressor qd = new QueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), qc.getCompressedBlocks());

            start = System.nanoTime();
            randomQueryTimeTest(qd,indexList);
            double queryChunkTime = (System.nanoTime() - start) / TIME_PRECISION;
            fileNameChunkToQueryTime.get(filename).add(queryChunkTime);

            // Chunk + BTree
            BTreeQueryCompressor btqc = new BTreeQueryCompressor(new SElfStarCompressor(new SElfXORCompressor()),filename);
            BTreeQueryDecompressor btqd = new BTreeQueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), btqc.getCompressedBlocksBTree());

            start = System.nanoTime();
            randomQueryTimeTest(btqd,indexList);
            double queryBTreeTime = (System.nanoTime() - start) / TIME_PRECISION;
            fileNameBTreeToQueryTime.get(filename).add(queryBTreeTime);
        }

    }


    public void testRandomQueryTimeConsumption(int num){
        int queryNumber = num;
        Random random = new Random();
        double start;

        for (String filename : fileNames){
            List<Double> values = readfile(filename);
            int numberOfData = values.size();
            List<Integer> indexList = new ArrayList<>();
            for (int i=0; i < queryNumber; i++){
                indexList.add(random.nextInt(numberOfData));
            }

            //Baseline: Time of decompress + Time of query
            double decompressTime = 0;
            ICompressor compressor = new SElfStarCompressor(new SElfXORCompressor());
            IDecompressor decompressor = new ElfStarDecompressor(new SElfStarXORDecompressor());
            try (BlockReader br = new BlockReader(filename, BLOCK_SIZE)) {
                List<Double> floatings;
                while ((floatings = br.nextBlock()) != null) {
                    if (floatings.size() != BLOCK_SIZE) {
                        break;
                    }
                    floatings.forEach(compressor::addValue);
                    compressor.close();
                    decompressor.setBytes(compressor.getBytes());
                    start = System.nanoTime();
                    List<Double> devalues = decompressor.decompress();
                    decompressTime += (System.nanoTime() - start) / TIME_PRECISION;
                    compressor.refresh();
                    decompressor.refresh();
                }
            } catch (Exception e) {
                throw new RuntimeException(filename, e);
            }
            start = System.nanoTime();
            for (int i:indexList){
                values.get(i);
            }
            double queryBaselineTime = (System.nanoTime() - start) / TIME_PRECISION + decompressTime;
            fileNameBaselineToQueryTime.get(filename).add(queryBaselineTime);

            //Chunk and Chunk+BTree
            QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()),filename);
            QueryDecompressor qd = new QueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), qc.getCompressedBlocks());

            BTreeQueryCompressor btqc = new BTreeQueryCompressor(new SElfStarCompressor(new SElfXORCompressor()),filename);
            BTreeQueryDecompressor btqd = new BTreeQueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), btqc.getCompressedBlocksBTree());

            start = System.nanoTime();
            randomQueryTimeTest(qd,indexList);
            double queryChunkTime = (System.nanoTime() - start) / TIME_PRECISION;
            fileNameChunkToQueryTime.get(filename).add(queryChunkTime);

            start = System.nanoTime();
            randomQueryTimeTest(btqd,indexList);
            double queryBTreeTime = (System.nanoTime() - start) / TIME_PRECISION;
            fileNameBTreeToQueryTime.get(filename).add(queryBTreeTime);
        }
    }

    @Test
    public void  testCompressionSpace(){
        int blockSize = 1000;
        for (String filename : fileNames){
            // Baseline: Buff
            long compressedBitsBuff = 0;
            try (BlockReader br = new BlockReader(filename,blockSize)){
                List<Double> floatings;
                while ((floatings = br.nextBlock()) != null){
                    if (floatings.size() != blockSize){
                        break;
                    }
                    double[] values = floatings.stream()
                            .mapToDouble(Double::doubleValue)
                            .toArray();
                    BuffCompressor compressor = new BuffCompressor(blockSize);
                    compressor.compress(values);
                    compressedBitsBuff += compressor.getSize();
                }
                fileNameBuffToCompressedBits.put(filename,compressedBitsBuff);
            } catch (Exception e){
                throw new RuntimeException(filename,e);
            }

            //Baseline: SElf
            long compressedBitsSElf = 0;
            ICompressor SElfCompressor =  new SElfStarCompressor(new SElfXORCompressor());
            try (BlockReader br = new BlockReader(filename,blockSize)){
                List<Double> floatings;
                while ((floatings = br.nextBlock()) != null) {
                    if (floatings.size() != blockSize) {
                        break;
                    }
                    SElfCompressor.refresh();
                    floatings.forEach(SElfCompressor::addValue);
                    compressedBitsSElf += SElfCompressor.getCompressedSizeInBits();
                }
                fileNameSElfToCompressedBits.put(filename, compressedBitsSElf);
            } catch (Exception e){
                throw new RuntimeException(filename,e);
            }

            // Chunk
            int blockDataCapacity = 2048;
            QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()),filename,blockDataCapacity);
            // as for one CompressedBlock = ( int * 3 + double * 2 + long * 1 ) + blockDataCapacity
            int compressedBlockBits = (4 * 3 + 8 * 3) * 8 + blockDataCapacity;
            long compressedBitsChunk = compressedBlockBits * qc.getCompressedBlocks().size();
            fileNameChunkToCompressedBits.put(filename, compressedBitsChunk);

            //Chunk+BTree
            BTreeQueryCompressor btqc = new BTreeQueryCompressor(new SElfStarCompressor(new SElfXORCompressor()),filename,blockDataCapacity);
            // as for one BTreeNode = (reference * 2 + boolean * 1 + int * 1)
            long BTreeBits = (8 * 2 + 1 + 4) * 8 * btqc.getCompressedBlocksBTree().getTreeNodeNumber();
            fileNameBTreeToCompressedBits.put(filename, compressedBitsChunk + BTreeBits);

        }

        // write result
        try {
            File file = new File(RESULT_Bits_CSV).getParentFile();
            if (!file.exists() && !file.mkdirs()) {
                throw new IOException("Create directory failed: " + file);
            }
            try (FileWriter writer = new FileWriter(RESULT_Bits_CSV, false)) {
                writer.write("Dataset,Buff, SElf, Chunk, Chunk+BTree");
                writer.write("\r\n");
                for (String filename : fileNames) {
                    writer.write(filename);
                    writer.write(",");
                    writer.write(fileNameBuffToCompressedBits.get(filename).toString());
                    writer.write(",");
                    writer.write(fileNameSElfToCompressedBits.get(filename).toString());
                    writer.write(",");
                    writer.write(fileNameChunkToCompressedBits.get(filename).toString());
                    writer.write(",");
                    writer.write(fileNameBTreeToCompressedBits.get(filename).toString());
                    writer.write("\r\n");
                }
                System.out.println("Done!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testBasicQuery(){
        String filename = "Air-pressure.csv";
        QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()),filename);
        QueryDecompressor qd = new QueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), qc.getCompressedBlocks());

        List<Double> floatings = readfile(filename);


        boolean testRandom = true;
        for (int i = 0; i < floatings.size();i++){
            String str1 = Double.toString(qd.RandomQuery(i));
            String str2 = Double.toString(randomQueryWithoutCompress(floatings,i));
            if (!str1.equals(str2)){
                testRandom = false;
                System.out.println(" "+ i);
                System.out.println("Test of random query fail!");
                System.out.println("The i is : " + i);
                System.out.println(str1);
                System.out.println(str2);
            }
        }
        System.out.println();
        if (testRandom){
            System.out.println("Test of random query pass!");
        }

//        int i = 365;
//        int start = 24;
//        int end = 44;
//
//        System.out.println(i+"-th data:"+qd.RandomQuery(i));
//        System.out.println(Long.toBinaryString(Double.doubleToLongBits(qd.RandomQuery(i))) + " " +Double.doubleToLongBits(qd.RandomQuery(i)));
//        System.out.println(i+"-th data:"+randomQueryWithoutCompress(floatings,i));
//        System.out.println(Long.toBinaryString(Double.doubleToLongBits(randomQueryWithoutCompress(floatings,i))) + " " + Double.doubleToLongBits(randomQueryWithoutCompress(floatings,i)));
//        System.out.println();

//        String str1 = Double.toString(qd.RandomQuery(i));
//        String str2 = Double.toString(randomQueryWithoutCompress(floatings,i));
//        if (!str1.equals(str2)){
//            System.out.println("random query error");
//            System.out.println(qd.RandomQuery(i));
//            System.out.println(randomQueryWithoutCompress(floatings,i));
//        }

//        System.out.println("max[start,end]:"+qd.MaxQuery(start,end));
//        System.out.println("max[start,end]:"+maxQueryWithoutCompress(floatings,start,end));
//        System.out.println();
//
//        System.out.println("min[start,end]:"+qd.MinQuery(start,end));
//        System.out.println("min[start,end]:"+minQueryWithoutCompress(floatings,start,end));
//        System.out.println();

        System.out.println("Test Complete!");
        System.gc();


    }

    @Test
    public void TestTreeQuery(){
        String filename = "Basel-temp.csv";
        BTreeQueryCompressor btqc = new BTreeQueryCompressor(new SElfStarCompressor(new SElfXORCompressor()),filename);
        BTreeQueryDecompressor btqd = new BTreeQueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), btqc.getCompressedBlocksBTree());

        List<Double> floatings = readfile(filename);

        boolean testRandom = true;
        for (int i = 0; i < floatings.size();i++){
            String str1 = Double.toString(btqd.RandomQuery(i));
            String str2 = Double.toString(randomQueryWithoutCompress(floatings,i));
            if (!str1.equals(str2)){
                testRandom = false;
                System.out.println(" "+ i);
                System.out.println("Test of random query fail!");
                System.out.println("The i is : " + i);
                System.out.println(str1);
                System.out.println(str2);
            }
        }
        System.out.println();
        if (testRandom){
            System.out.println("Test of random query pass!");
        }

//        int i = 2500;
//        int start = 24;
//        int end = 26;
//
//        System.out.println(i+"-th data:"+btqd.RandomQuery(i));
//        System.out.println(i+"-th data:"+randomQueryWithoutCompress(floatings,i));
//        if (!String.valueOf(btqd.RandomQuery(i)).equals(String.valueOf(randomQueryWithoutCompress(floatings,i)))){
//            System.out.println("random query error");
//        }
//        System.out.println();
//
//        System.out.println("max[start,end]:"+btqd.MaxQuery(start,end));
//        System.out.println("max[start,end]:"+maxQueryWithoutCompress(floatings,start,end));
//        System.out.println();
//
//        System.out.println("min[start,end]:"+btqd.MinQuery(start,end));
//        System.out.println("min[start,end]:"+minQueryWithoutCompress(floatings,start,end));
//        System.out.println();

        System.out.println("Test Complete!");
        System.gc();
    }

    public boolean randomQueryIfRight(IQueryDecompressor qd, String filename){
        List<Double> floatings = readfile(filename);
        boolean testRandom = true;
        for (int i = 0; i < floatings.size();i++){
            String str1 = Double.toString(qd.RandomQuery(i));
            String str2 = Double.toString(randomQueryWithoutCompress(floatings,i));
            if (!str1.equals(str2)){
                testRandom = false;
                System.out.println(" "+ i);
                System.out.println("Test of random query fail!");
                System.out.println("The i is : " + i);
                System.out.println(str1);
                System.out.println(str2);
            }
        }
        return testRandom;
    }

    public void randomQueryTimeTest(IQueryDecompressor qd, List<Integer> indexListForQuery){
        for (int i:indexListForQuery){
            qd.RandomQuery(i);
        }
    }


    public List<Double> readfile(String filename) {
        List<Double> floatings = new ArrayList<>();
        try (BlockReader br = new BlockReader(filename, 1000)) {
            List<Double> tmp;
            while ((tmp = br.nextBlock()) != null){
                floatings.addAll(tmp);
            }
        } catch (Exception e) {
            throw new RuntimeException(filename, e);
        }
        return floatings;
    }

    public Double randomQueryWithoutCompress(List<Double> floatings, int i){
        return floatings.get(i);
    }

    public Double minQueryWithoutCompress(List<Double> floatings, int s, int e){
        Double min = Double.MAX_VALUE;
        for (int i = s; i<=e;i++){
            if(floatings.get(i) < min){
                min = floatings.get(i);
            }
        }
        return  min;
    }

    public Double maxQueryWithoutCompress (List<Double> floatings, int s, int e){
        Double max = Double.MIN_VALUE;
        for (int i=s;i<=e;i++){
            if (floatings.get(i) > max){
                max = floatings.get(i);
            }
        }
        return max;
    }




    // 将压缩块写入文件
    public void writeBytesToFile(byte[] data, String filePath) {
        try (FileOutputStream fos = new FileOutputStream(filePath);
             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.writeObject(data); // 将压缩块的数据写入文件
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 从文件中读取压缩块并返回
    public byte[] readBytesFromFile(String filePath) {
        try (FileInputStream fis = new FileInputStream(filePath);
             ObjectInputStream ois = new ObjectInputStream(fis)) {
            byte[] data = (byte[]) ois.readObject(); // 从文件中读取压缩块的数据
            return data; // 创建并返回压缩块对象
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void initQueryTimeMap(){
        for (String filename: fileNames){
            fileNameBaselineToQueryTime.put(filename,new ArrayList<>());
            fileNameChunkToQueryTime.put(filename,new ArrayList<>());
            fileNameBTreeToQueryTime.put(filename,new ArrayList<>());
        }
    }
    private List<String> createFiles(int fileNumber){
        File folder = new File(folderPath_Bytes);
        List<String> BytesFiles = new ArrayList<>();
        if (!folder.exists()) {
            folder.mkdirs();
        }

        for (int i = 1; i <= fileNumber; i++) {
            String fileName = "fileBytes_" + i + ".txt";
            File file = new File(folderPath_Bytes + fileName);
            try {
                if (file.createNewFile()) {
                    BytesFiles.add(fileName);
                } else {
                    System.out.println("The file already exists" + fileName);
                }
            } catch (IOException e) {
                System.out.println("Fail to create the file:" + fileName);
                e.printStackTrace();
            }
        }
        return BytesFiles;

    }
    private void refreshFiles(int fileNumber){
        for (int i = 1; i <= fileNumber; i++) {
            String fileName = "fileBytes_" + i + ".txt";
            File file = new File(folderPath_Bytes + fileName);
            try {
                // 使用 FileWriter 打开文件，并传入 true 参数表示追加写入
                FileWriter writer = new FileWriter(file, false);
                // 将文件内容清空
                writer.write("");
                // 关闭文件写入流
                writer.close();
                System.out.println("文件内容已清空：" + fileName);
            } catch (IOException e) {
                System.out.println("文件内容清空失败：" + fileName);
                e.printStackTrace();
            }
        }
    }
}


