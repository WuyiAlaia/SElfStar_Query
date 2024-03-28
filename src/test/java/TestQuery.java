import org.junit.jupiter.api.Test;
import org.urbcomp.startdb.selfstar.compressor.ICompressor;
import org.urbcomp.startdb.selfstar.compressor.SElfStarCompressor;
import org.urbcomp.startdb.selfstar.compressor.query.QueryCompressor;
import org.urbcomp.startdb.selfstar.compressor.xor.SElfXORCompressor;
import org.urbcomp.startdb.selfstar.decompressor.ElfStarDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.IDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.query.QueryDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.xor.SElfStarXORDecompressor;
import org.urbcomp.startdb.selfstar.utils.BlockReader;

import java.util.ArrayList;
import java.util.List;



public class TestQuery {
    @Test
    public void TestBasicQuery(){
        String filename = "Air-pressure.csv";
        QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()),filename);
        QueryDecompressor dc = new QueryDecompressor(new ElfStarDecompressor(new SElfStarXORDecompressor()), qc.getCompressedBlocks());

        List<Double> floatings = readfile(filename);

        int i = 16;
        int start = 160;
        int end = 55169;
        System.out.println(i+"-th data:"+dc.RandomQuery(i));
        System.out.println(i+"-th data:"+R(floatings,i));
        System.out.println();

//        System.out.println("max[start,end]:"+dc.MaxQuery(start,end));
//        System.out.println("max[start,end]:"+Max(floatings,start,end));
//        System.out.println();
//
//        System.out.println("min[start,end]:"+dc.MinQuery(start,end));
//        System.out.println("min[start,end]:"+Min(floatings,start,end));
//        System.out.println();

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


