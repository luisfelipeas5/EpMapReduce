package br.com.mapreduce.leastsquare;

import br.com.mapreduce.Constants;
import br.com.mapreduce.dategrep.DateGrepJob;
import br.com.mapreduce.stationgrep.StationGrepJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LeastSquareJob extends Configured implements Tool {
    public static final String NAME = "LeastSquareJob";
    private static final int RESULT_CODE_FAILED = 0;
    public static final int RESULT_CODE_SUCCESS = 1;

    public int run(String[] args) throws Exception {
        if(args.length < 6){
            System.out.println(Constants.COMMAND_ARGUMENTS_LEAST_SQUARE);
            //arguments are not enough, input and outputs paths must be passed in the firsts parameters
            throw new CommandFormat.NotEnoughArgumentsException(2, args.length);
        }
        Path inputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        String stationNumber = args[3];
        String dateBegin = args[4];
        String dateEnd = args[5];

        //Set params of job inside the Configuration
        Configuration configuration = getConf();

        //If the user put a work station filter as parameter, we have to run the job to filter this
        if(stationNumber != null && !stationNumber.equals("") ) {
            //if the filter by date was succeed, update the inputPath for the next Job work
            inputPath = runStationGrepJob(inputPath, outputPath, stationNumber);
        }

        //If the user put a work begin date and end date as parameter, we have to run the job to filter this
        if(dateBegin != null &&  !dateBegin.equals("") && dateEnd != null &&  !dateEnd.equals("") ) {
            //if the filter by date was succeed, update the inputPath for the next Job work
            inputPath = runDateGrepJob(inputPath, outputPath, dateBegin, dateEnd);
        }

        Job leastSquareJob = new Job(configuration);
        leastSquareJob.setJarByClass(getClass());
        leastSquareJob.setJobName(NAME);

        FileInputFormat.addInputPath(leastSquareJob, inputPath);
        FileOutputFormat.setOutputPath(leastSquareJob, outputPath);

        leastSquareJob.setMapperClass(LeastSquareMapper.class);
        leastSquareJob.setCombinerClass(LeastSquareReducer.class);
        leastSquareJob.setReducerClass(LeastSquareReducer.class);

        leastSquareJob.setOutputKeyClass(Text.class);
        leastSquareJob.setOutputValueClass(DoubleWritable.class);

        boolean completed = leastSquareJob.waitForCompletion(true);
        if(completed){
            return RESULT_CODE_SUCCESS;
        }
        return RESULT_CODE_FAILED;
    }

    private Path runDateGrepJob(Path inputPath, Path outputPath, String dateBegin, String dateEnd) {
        DateGrepJob dateGrepJob = new DateGrepJob();
        int runCode;
        try {
            runCode = ToolRunner.run(dateGrepJob, new String[]{inputPath.getName(), outputPath.getName(), dateBegin, dateEnd});
            if(runCode == DateGrepJob.RESULT_CODE_SUCCESS) {
                System.out.println(DateGrepJob.NAME + " success :)");
                return outputPath;
            } else {
                System.out.println(DateGrepJob.NAME + " failed :(");
                return inputPath;
            }
        } catch (Exception e) {
            System.out.println("Error executing " + DateGrepJob.NAME);
            e.printStackTrace();
            return inputPath;
        }
    }

    private Path runStationGrepJob(Path inputPath, Path outputPath, String stationNumber) {
        StationGrepJob stationGrepJob = new StationGrepJob();
        int runCode;
        try {
            runCode = ToolRunner.run(stationGrepJob, new String[]{inputPath.getName(), outputPath.getName(), stationNumber});
            if(runCode == StationGrepJob.RESULT_CODE_SUCCESS) {
                System.out.println(StationGrepJob.NAME + " success :)");
                return outputPath;
            } else {
                System.out.println(StationGrepJob.NAME + " failed :(");
                return inputPath;
            }
        } catch (Exception e) {
            System.out.println("Error executing " + StationGrepJob.NAME);
            e.printStackTrace();
            return inputPath;
        }
    }
}
