package br.com.mapreduce.stationgrep;

import br.com.mapreduce.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class StationGrepJob extends Configured implements Tool{
    public static final String NAME = "StationGrepJob";
    public static final int RESULT_CODE_SUCCESS = 1;
    private static final int RESULT_CODE_FAILED = 0;
    static final String CONF_NAME_STATION = "CONF_NAME_STATION";

    public int run(String[] args) throws Exception {
        if(args.length < 3){
            System.out.println(Constants.COMMAND_ARGUMENTS_STATION_GREP);
            //arguments are not enough, input and outputs paths must be passed in the firsts parameters
            throw new CommandFormat.NotEnoughArgumentsException(3, args.length);
        }
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        String stationNumber = args[2];

        Configuration configuration = getConf();
        configuration.setInt(StationGrepJob.CONF_NAME_STATION, Integer.parseInt(stationNumber));

        Job statioGrepJob = new Job(configuration);
        statioGrepJob.setJarByClass(getClass());
        statioGrepJob.setJobName(NAME);

        FileInputFormat.setInputPaths(statioGrepJob, inputPath);
        FileOutputFormat.setOutputPath(statioGrepJob, outputPath);

        statioGrepJob.setMapperClass(StationMapper.class);
        statioGrepJob.setReducerClass(StationReducer.class);

        statioGrepJob.setMapOutputKeyClass(LongWritable.class);
        statioGrepJob.setMapOutputValueClass(Text.class);

        statioGrepJob.setOutputValueClass(Text.class);
        statioGrepJob.setOutputValueClass(Text.class);

        boolean completed = statioGrepJob.waitForCompletion(true);
        if(completed){
            return RESULT_CODE_SUCCESS;
        }
        return RESULT_CODE_FAILED;
    }
}
