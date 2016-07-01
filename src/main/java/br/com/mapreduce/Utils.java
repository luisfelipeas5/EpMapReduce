package br.com.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;
import java.util.Map;
import java.util.HashMap;

public class Utils {
    private static Map<String, Double> dadosInvalidos;

    public Utils() {
        dadosInvalidos = new HashMap<String, Double>();
        dadosInvalidos.put("TEMP", 9999.9);
        dadosInvalidos.put("DEWP", 9999.9);
        dadosInvalidos.put("SLP", 9999.9);
        dadosInvalidos.put("STP", 9999.9);
        dadosInvalidos.put("VISIB", 999.9);
        dadosInvalidos.put("WDSP", 999.9);
        dadosInvalidos.put("GUST", 999.9);
        dadosInvalidos.put("MAX", 9999.9);
        dadosInvalidos.put("MIN", 9999.9);
        dadosInvalidos.put("PRCP", 99.99);
        dadosInvalidos.put("SNDP", 999.9);
    }

    public static Scanner getScanner(String outputPath) throws IOException {
        Path pt = new Path(outputPath + Path.SEPARATOR + "part-r-00000");
        FileSystem fs = FileSystem.get(new Configuration());
        return new Scanner(new InputStreamReader(fs.open(pt)));
    }

    public static double getInvalidData(String abbreviation) {
        return dadosInvalidos.get(abbreviation);
    }

}
