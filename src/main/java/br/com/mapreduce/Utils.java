package br.com.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

public class Utils {
    public static Scanner getScanner(String outputPath) throws IOException {
        Path pt = new Path(outputPath + Path.SEPARATOR + "part-r-00000");
        FileSystem fs = FileSystem.get(new Configuration());
        return new Scanner(new InputStreamReader(fs.open(pt)));
    }
}
