package br.com.mapreduce.leastsquare;

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

public class LeastSquareJob extends Configured implements Tool {
    public static final String NAME = "LeastSquareJob";
    private static final int RESULT_CODE_FAILED = 0;
    public static final int RESULT_CODE_SUCCESS = 1;

    public int run(String[] strings) throws Exception {
        if(strings.length < 3){
            System.out.println("");
            //arguments are not enough, input and outputs paths must be passed in the firsts parameters
            throw new CommandFormat.NotEnoughArgumentsException(2, strings.length);
        }

        //Set params of job inside the Configuration
        Configuration configuration = getConf();

        Job leastSquareJob = new Job(configuration);
        leastSquareJob.setJarByClass(getClass());
        leastSquareJob.setJobName(getClass().getSimpleName());

        //Add the path of the files
        String inputPath = strings[1];
        FileInputFormat.addInputPath(leastSquareJob, new Path(inputPath));
        String outputPath = strings[2];
        FileOutputFormat.setOutputPath(leastSquareJob,  new Path(outputPath));

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
}
