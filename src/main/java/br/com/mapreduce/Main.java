package br.com.mapreduce;

import org.apache.hadoop.util.ToolRunner;

public class Main {

    public static void main(String[] args){
        if(args.length > 0) {
            String command = args[0];
            if (command.equals(Constants.COMMAND_LEAST_SQUARE)) {
                runLeastSquare(args);
            } else { //When the first arguments doesn't match with any command option
                printManual();
            }
        } else{
            printManual();
        }
    }

    static void printManual(){
        //TODO - print command options and explanation
        System.out.println("Command options:");
        System.out.println("\t" + Constants.COMMAND_LEAST_SQUARE + " - " + Constants.COMMAND_EXPLANATION_LEAST_SQUARE);
    }

    private static void runLeastSquare(String[] args){
        LeastSquareJob leastSquareJob = new LeastSquareJob();
        int runCode;
        try {
            runCode = ToolRunner.run(leastSquareJob, args);
            if(runCode == LeastSquareJob.RESULT_CODE_SUCCESS) {
                System.out.println(LeastSquareJob.NAME + " success :)");
            } else {
                System.out.println(LeastSquareJob.NAME + " failed :(");
            }
        } catch (Exception e) {
            System.out.println("Error executing " + LeastSquareJob.NAME);
            e.printStackTrace();
        }
    }

}
