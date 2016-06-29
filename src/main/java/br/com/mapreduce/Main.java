package br.com.mapreduce;

import br.com.mapreduce.leastsquare.LeastSquareJob;
import br.com.mapreduce.mean.MeanJob;
import org.apache.hadoop.util.ToolRunner;

public class Main {
    public static void main(String[] args){
        if(args.length > 0) {
            String command = args[0];
            if (command.equals(Constants.COMMAND_LEAST_SQUARE)) {
                runLeastSquare(args);
            }
            else if (command.equals(Constants.COMMAND_MEAN)) {
                runMean(args);
            }else { //When the first arguments doesn't match with any command option
                printManual();
            }
        }
        else{
            printManual();
        }
    }

    private static void printManual(){
        //TODO - print command options and explanation
        System.out.println("Command options:");
        System.out.println("\t" + Constants.COMMAND_LEAST_SQUARE + " - " + "\n\t" + Constants.COMMAND_ARGUMENTS_LEAST_SQUARE +
                "\n\t" + Constants.COMMAND_EXPLANATION_LEAST_SQUARE);
        System.out.println("\t" +Constants.COMMAND_MEAN + " - " +"\n\t" +Constants.COMMAND_ARGUMENTS_MEAN +
                "\n\t" +Constants.COMMAND_EXPLANATION_MEAN);
    }

    private static void runLeastSquare(String[] args){
        LeastSquareJob leastSquareJob = new LeastSquareJob();
        int runCode;
        try {
            runCode = ToolRunner.run(leastSquareJob, args);
            if(runCode == LeastSquareJob.RESULT_CODE_SUCCESS) {
                System.out.println(LeastSquareJob.NAME + " success :)");
                //TODO Load file
                //TODO format data
                //TODO return
            } else {
                System.out.println(LeastSquareJob.NAME + " failed :(");
            }
        } catch (Exception e) {
            System.out.println("Error executing " + LeastSquareJob.NAME);
            e.printStackTrace();
        }
    }

    private static void runMean(String[] args){
        MeanJob meanJob = new MeanJob();
        try {
            int runCode = ToolRunner.run(meanJob, args);
            if(runCode == MeanJob.RESULT_CODE_SUCCESS) {
                System.out.println(MeanJob.NAME + " success :)");
                //TODO Load file
                //TODO format data
                //TODO return
            } else {
                System.out.println(MeanJob.NAME + " failed :(");
            }
        } catch (Exception e) {
            System.out.println("Error executing " + MeanJob.NAME);
            e.printStackTrace();
        }
    }
}
