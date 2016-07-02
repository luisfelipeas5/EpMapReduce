package br.com.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.Scanner;
import java.util.Map;
import java.util.HashMap;

import java.io.IOException;
import java.util.zip.GZIPInputStream;

public class Utils {
    private static Map<String, Double> dadosInvalidos;

    public static void inicializaDadosInvalidos() {
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
        return dadosInvalidos.get(abbreviation);
    }
}
