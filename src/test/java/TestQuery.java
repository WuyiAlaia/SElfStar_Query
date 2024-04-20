import org.junit.jupiter.api.Test;
import org.urbcomp.startdb.selfstar.compressor.ICompressor;
import org.urbcomp.startdb.selfstar.compressor.SElfStarCompressor;
import org.urbcomp.startdb.selfstar.compressor.query.BTreeQueryCompressor;
import org.urbcomp.startdb.selfstar.compressor.query.QueryCompressor;
import org.urbcomp.startdb.selfstar.compressor.xor.SElfXORCompressor;
import org.urbcomp.startdb.selfstar.decompressor.ElfStarDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.IDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.query.BTreeQueryDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.query.QueryDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.xor.SElfStarXORDecompressor;
import org.urbcomp.startdb.selfstar.utils.BlockReader;

import java.util.ArrayList;
import java.util.List;



public class TestQuery {
    @Test
    public void TestBasicQuery(){
        String filename = "Air-sensor.csv";
        QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()),filename);
        QueryDecompressor qd = new QueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), qc.getCompressedBlocks());

        List<Double> floatings = readfile(filename);

        int i = 19;
        int start = 24;
        int end = 81;
        System.out.println(i+"-th data:"+qd.RandomQuery(i));
        System.out.println(i+"-th data:"+R(floatings,i));
        System.out.println();

//        System.out.println("max[start,end]:"+qd.MaxQuery(start,end));
//        System.out.println("max[start,end]:"+Max(floatings,start,end));
//        System.out.println();
//
//        System.out.println("min[start,end]:"+qd.MinQuery(start,end));
//        System.out.println("min[start,end]:"+Min(floatings,start,end));
//        System.out.println();

        System.out.println("Test Complete!");
        System.gc();


    }

    @Test
    public void TestTreeQuery(){
        String filename = "Air-pressure.csv";
        BTreeQueryCompressor btqc = new BTreeQueryCompressor(new SElfStarCompressor(new SElfXORCompressor()),filename);
        BTreeQueryDecompressor btqd = new BTreeQueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), btqc.getCompressedBlocksBTree());

        List<Double> floatings = readfile(filename);

        int i = 16;
        int start = 24;
        int end = 81;
        System.out.println(i+"-th data:"+btqd.RandomQuery(i));
        System.out.println(i+"-th data:"+R(floatings,i));
        System.out.println();

        System.out.println("max[start,end]:"+btqd.MaxQuery(start,end));
        System.out.println("max[start,end]:"+Max(floatings,start,end));
        System.out.println();

        System.out.println("min[start,end]:"+btqd.MinQuery(start,end));
        System.out.println("min[start,end]:"+Min(floatings,start,end));
        System.out.println();

        System.out.println("Test Complete!");
        System.gc();



        System.out.println("Test Complete!");
        System.gc();
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

    public Double R(List<Double> floatings, int i){
        return floatings.get(i);
    }

    public Double Min(List<Double> floatings, int s, int e){
        Double min = Double.MAX_VALUE;
        for (int i = s; i<=e;i++){
            if(floatings.get(i) < min){
                min = floatings.get(i);
            }
        }
        return  min;
    }

    public Double Max (List<Double> floatings, int s, int e){
        Double max = Double.MIN_VALUE;
        for (int i=s;i<=e;i++){
            if (floatings.get(i) > max){
                max = floatings.get(i);
            }
        }
        return max;
    }
}


