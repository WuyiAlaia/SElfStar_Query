import org.junit.jupiter.api.Test;
import org.urbcomp.startdb.selfstar.compressor.ElfStarCompressor;
import org.urbcomp.startdb.selfstar.compressor.ICompressor;
import org.urbcomp.startdb.selfstar.compressor.SElfStarCompressor;
import org.urbcomp.startdb.selfstar.compressor.xor.ElfStarXORCompressor;
import org.urbcomp.startdb.selfstar.compressor.xor.SElfXORCompressor;
import org.urbcomp.startdb.selfstar.decompressor.ElfStarDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.IDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.xor.ElfStarXORDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.xor.SElfStarXORDecompressor;
import org.urbcomp.startdb.selfstar.query.CompressAndQuery;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQuery {
    @Test
    public void TestBasicQuery(){
        ICompressor compressor = new SElfStarCompressor(new SElfXORCompressor());
        String filename = "Air-pressure.csv";
        CompressAndQuery blocks = new CompressAndQuery(compressor,filename);
        IDecompressor decompressor = new ElfStarDecompressor(new SElfStarXORDecompressor());

        int i = 16;
        int j = 24;
        System.out.println("i-th data:");
        blocks.RandomQuery(decompressor,i);
        System.out.println("max[i,j]:");
        blocks.MaxQuery(decompressor,i,j);
        System.out.println("min[i,j]:");
        blocks.MinQuery(decompressor,i,j);
        System.out.println("Test Complete!");
        System.gc();
    }
}
