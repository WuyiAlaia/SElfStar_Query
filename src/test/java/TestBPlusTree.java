import org.junit.jupiter.api.Test;
import org.urbcomp.startdb.selfstar.compressor.ICompressor;
import org.urbcomp.startdb.selfstar.compressor.SElfStarChunkCompressor;
import org.urbcomp.startdb.selfstar.compressor.xor.SElfXORCompressor;
import org.urbcomp.startdb.selfstar.decompressor.IDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.SElfStarDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.xor.SElfStarXORDecompressor;
import org.urbcomp.startdb.selfstar.query.BPlusTree;
import org.urbcomp.startdb.selfstar.query.CompressedBlock;
import org.urbcomp.startdb.selfstar.utils.BlockFileReader;
import org.urbcomp.startdb.selfstar.query.BPlusTreeSerializer;
import org.urbcomp.startdb.selfstar.utils.ByteArrayFileReader;

import java.io.*;
import java.util.*;

public class TestBPlusTree {
    private static final String folderPath_Bytes_Tree = "D:/bytes/TreeBytes/";
    private static final String folderPath_Bytes_Chunk = "D:/bytes/ChunkBytes/";
    private static final double TIME_PRECISION = 1000.0;
    private static final int BLOCK_SIZE = 10;
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
    @Test
    public void testBaseline() throws IOException {
        int queryIndex;
        Random random = new Random();

        String filename = "Air-pressure.csv";
        List<Double> floatingsReadFromFile = readfile(filename);
        queryIndex = random.nextInt(floatingsReadFromFile.size());
        //Baseline
        ICompressor compressor = new SElfStarChunkCompressor(new SElfXORCompressor());
        IDecompressor decompressor = new SElfStarDecompressor(new SElfStarXORDecompressor());
        File fileForBaseline = createFiles("SElfCompressor",filename);
        int timesOfWrite = 0;
        int compressNUmber = 0;
        try (BlockReader br = new BlockReader(filename, BLOCK_SIZE)) {
            List<Double> floatings;
            while ((floatings = br.nextBlock()) != null) {
                floatings.forEach(compressor::addValue);
                compressNUmber += floatings.size();
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
            decompressor.refresh();
            decompressor.setBytes(data);
            List<Double> floatingsOneTime = decompressor.decompress();
            for (Double floating : floatingsOneTime){
                allFloatings.add(floating);
            }
        }
        allFloatings.get(queryIndex);
        System.out.println("The numbers when compress:" + compressNUmber);
        System.out.println("The numbers after compression:" + allFloatings.size());
        System.out.println("The numbers in file:" + floatingsReadFromFile.size());
        double baselineRandomQueryTime = (System.nanoTime() - start) / TIME_PRECISION;
        System.out.println(baselineRandomQueryTime);


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



    @Test
    public void testReadFiles(){
        BPlusTree tree = new BPlusTree(3);
        tree.insert(0,"0.txt");
        tree.insert(1,"1.txt");
        tree.insert(2,"2.txt");
        tree.insert(3,"3.txt");
        tree.insert(4,"4.txt");
        tree.insert(5,"5.txt");
        tree.insert(6,"6.txt");
        tree.insert(7,"7.txt");
        BPlusTreeSerializer.serialize(tree, "D:/bplus_tree_index.txt");
        BPlusTree loadedBPlusTree = BPlusTreeSerializer.deserialize("D:/bplus_tree_index.txt");
        System.out.println("hh");
    }
    @Test
    public void testReader(){
        CompressedBlock block = new CompressedBlock();
        block.resetIData(0);
        block.resetMinValue(88.51706);
        block.resetMaxValue(88.54805);
        block.resetWrittenBitSize(1021);
        byte[] data = new byte[] {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20};
        block.resetData(data,168);
        BlockFileReader bfr = new BlockFileReader();
        File file = new File("D:/bytes/test.txt");
        block.writeToFile(file);
        System.out.println(block.readMinValueFromFile(file));
    }
    @Test
    public  void testPath(){
        // 定义文件路径
        String path = "D:/bytes/TreeBytes/1092.txt";

        // 获取最后一个文件名分隔符的索引
        int lastSeparatorIndex = path.lastIndexOf("/"); // Unix/Linux
        if (lastSeparatorIndex == -1) {
            lastSeparatorIndex = path.lastIndexOf("\\"); // Windows
        }

        // 如果找到分隔符，则从分隔符后面的部分提取文件名
        String fileName = null;
        if (lastSeparatorIndex != -1) {
            fileName = path.substring(lastSeparatorIndex + 1);
        } else {
            // 如果没有找到分隔符，则整个路径都是文件名
            fileName = path;
        }

        // 输出文件名
        System.out.println("File Name: " + fileName);
    }

    @Test
    public void testFiles(){
        List<File> files = new ArrayList<>();
        files.add(new File("D:/testReadWrite/Air-pressure.csv/1.txt"));
        files.add(new File("D:/testReadWrite/Air-pressure.csv/2.txt"));
        files.add(new File("D:/testReadWrite/Air-pressure.csv/3.txt"));
        writeFilesToFile(files,"Air-pressure.csv");
        List<File> newfiles = readFilesFromFile("Air-pressure.csv");
        for (File file : newfiles){
            System.out.println(file.getName());
        }
    }


    private List<File> readFilesFromFile(String datasetFileName) {
        List<File> newFiles = new ArrayList<>();
        File catalogFile = new File("D:/testReadWrite/" + datasetFileName + "/" + "blockFiles");
        try (FileInputStream fileIn = new FileInputStream(catalogFile);
             ObjectInputStream in = new ObjectInputStream(fileIn)) {
            newFiles = (List<File>) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return newFiles;
    }
    private void writeFilesToFile(List<File> blockFiles, String datasetFile) {
        File folder = new File("D:/testReadWrite/" + datasetFile + "/");
        if (!folder.exists()) {
            folder.mkdirs();
        }
        File catalogFile = new File("D:/testReadWrite/" + datasetFile + "/" + "blockFiles");
        try {
            if (!catalogFile.exists()){
                catalogFile.createNewFile();
            }
            else {
                boolean ifClear = clearFile(catalogFile);
                if (!ifClear){
                    System.out.println("Fail to clear the file" );
                }
            }
        } catch (IOException e) {
            System.out.println("Fail to create the file" );
            e.printStackTrace();
        }

        try (FileOutputStream fileOut = new FileOutputStream(catalogFile);
             ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            out.writeObject(blockFiles);
        } catch (IOException e) {
            e.printStackTrace();
        }


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




}
