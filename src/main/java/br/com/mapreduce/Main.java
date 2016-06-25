package br.com.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Main {
    public static void main(String[] args){
        try {
            runLeastSquare(args);
        } catch (Exception e) {
            System.out.println("Error executing " + LeastSquareJob.NAME);
            e.printStackTrace();
        }
    }

    private static void runLeastSquare(String[] args) throws Exception {
        LeastSquareJob leastSquareJob = new LeastSquareJob();
        Configuration configuration = new Configuration();
        leastSquareJob.setConf(configuration);

        int runCode = ToolRunner.run(leastSquareJob, args);

        if(runCode == LeastSquareJob.RESULT_CODE_SUCCESS) {
            System.out.println(LeastSquareJob.NAME + " success :)");
        } else {
            System.out.println(LeastSquareJob.NAME + " failed :(");
        }
    }

}
