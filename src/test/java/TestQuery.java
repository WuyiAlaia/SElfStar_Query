import org.junit.jupiter.api.Test;
import org.urbcomp.startdb.selfstar.compressor.SElfStarCompressor;
import org.urbcomp.startdb.selfstar.compressor.query.QueryCompressor;
import org.urbcomp.startdb.selfstar.compressor.xor.SElfXORCompressor;
import org.urbcomp.startdb.selfstar.decompressor.SElfStarDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.query.QueryDecompressor;
import org.urbcomp.startdb.selfstar.decompressor.xor.SElfStarXORDecompressor;
import org.urbcomp.startdb.selfstar.query.QueryProcess;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQuery {

    private static final String INIT_FILE = "init.csv";
    private static final String COMPRESSED_CHUNK_FILE = "src/test/resources/result/";
    private static final String FILE_RESOURCES = "src/main/resources/floating/";

    private final String[] fileNames = {
//            INIT_FILE,
//            "Air-pressure.csv",
////            "Air-sensor.csv",
//            "Bird-migration.csv",
////            "Bitcoin-price.csv",
//            "Basel-temp.csv",
//            "Basel-wind.csv",
//            "City-temp.csv",
//            "Dew-point-temp.csv",
//            "IR-bio-temp.csv",
//            "PM10-dust.csv",
//            "Stocks-DE.csv",
//            "Stocks-UK.csv",
//            "Stocks-USA.csv",
//            "Wind-Speed.csv",
//            "Blockchain-tr.csv",
//            "City-lat.csv",
//            "City-lon.csv",
//            "Food-price.csv",
//            "POI-lat.csv",
//            "POI-lon.csv",
////            "SSD-bench.csv",
            "electric_vehicle_charging.csv"
//    "test.csv"
    };

    @Test
    public void testQuery() {
        for (String fileName : fileNames) {
            testQuery(FILE_RESOURCES + fileName, 1000);
        }
    }

    public void testQuery(String fileName, int ChunkSize) {
        List<Double> floatings = BlockReader.readCSV(fileName);
        QueryCompressor qc = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), ChunkSize);
        qc.compressAll(floatings);
        qc.saveToFile(fileName);
        QueryProcess queryProcess = new QueryProcess();

        for (int start = 0; start < floatings.size(); ++start) {
            double trueResult = floatings.get(start);
            for (int end = start; end < floatings.size(); ++end) {
                for(int i=start; i<=end; i++){
                    trueResult = Math.max(trueResult, floatings.get(i));
                }
                double ourResult = queryProcess.max(fileName, start, end);
//                double trueResult = maxDouble(floatings, start, end);
                if (ourResult != trueResult) {
                    System.out.println("start = " + start + " end = " + end);
                    System.out.println(ourResult);
                    System.out.println(trueResult);
                    return;
                }
                assertEquals(ourResult, trueResult);

            }
        }

//        for (int i = start; i < end; i++) {
//
//        }
    }

    public void compressAndSave(String fileName, int ChunkSize) {
        List<Double> floatings = BlockReader.readCSV(fileName);
        QueryCompressor compressor = new QueryCompressor(new SElfStarCompressor(new SElfXORCompressor()), ChunkSize);
        compressor.compressAll(floatings);
        compressor.saveToFile(fileName);
    }

    private double maxDouble(List<Double> floatings, int start, int end) {
        double max = Double.MIN_VALUE;
        for (int i = start; i <= end; i++) {
            if (floatings.get(i) > max) {
                max = floatings.get(i);
            }
        }
        return max;
    }
}
