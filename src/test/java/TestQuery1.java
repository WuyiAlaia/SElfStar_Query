import org.junit.jupiter.api.Test;
import org.urbcomp.startdb.selfstar.compressor.SElfStarCompressor;
import org.urbcomp.startdb.selfstar.compressor.query.QueryCompressor;
import org.urbcomp.startdb.selfstar.compressor.xor.SElfXORCompressor;
import org.urbcomp.startdb.selfstar.decompressor.IDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.SElfStarDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.query.QueryDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.xor.SElfStarXORDecompressor;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQuery1 {

    private static final String INIT_FILE = "init.csv";
    private static final String COMPRESSED_CHUNK_FILE = "src/test/resources/result/";

    private final String[] fileNames = {
            INIT_FILE,
            "Air-pressure.csv",
//            "Air-sensor.csv",
            "Bird-migration.csv",
//            "Bitcoin-price.csv",
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
//            "SSD-bench.csv",
//            "electric_vehicle_charging.csv"
    };


    public void testQuery(String fileName, int blockSize, int ChunkSize) {
        try (BlockReader br = new BlockReader(fileName, blockSize)) {
            List<Double> floatings;
            QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), ChunkSize);
            while ((floatings = br.nextBlock()) != null) {
                for (double floating : floatings) {
                    qc.compress(floating);
                }
            }
            qc.writeChunksToFile(COMPRESSED_CHUNK_FILE + fileName);

            QueryDecompressor qd = new QueryDecompressor(new SElfStarDecompressor(new SElfStarXORDecompressor()));


        } catch (Exception e) {
            throw new RuntimeException(fileName, e);
        }

    }
}
