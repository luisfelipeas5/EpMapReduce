package br.com.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

class LeastSquareJob implements Tool {
    static final String NAME = "LeastSquareJob";
    private static final int RESULT_CODE_FAILED = 0;
    static final int RESULT_CODE_SUCCESS = 1;
    private Configuration configuration;

    public int run(String[] strings) throws Exception {
        //Set params of job inside the Configuration
        Configuration configuration = getConf();

        Job leastSquareJob = new Job(configuration);
        leastSquareJob.setJarByClass(getClass());
        leastSquareJob.setJobName(getClass().getSimpleName());


        String inputPath = strings[0];
        FileInputFormat.addInputPath(leastSquareJob, new Path(inputPath));
        String outputPath = strings[1];
        FileOutputFormat.setOutputPath(leastSquareJob, new Path(outputPath));

        leastSquareJob.setMapperClass(LeastSquareMapper.class);
        leastSquareJob.setCombinerClass(LeastSquareReducer.class);
        leastSquareJob.setReducerClass(LeastSquareReducer.class);

        leastSquareJob.setOutputKeyClass(Text.class);
        leastSquareJob.setOutputValueClass(DoubleWritable.class);

        //return leastSquareJob.waitForCompletion(true) ? RESULT_CODE_FAILED : RESULT_CODE_SUCCESS;
        return RESULT_CODE_FAILED;
    }

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConf() {
        return configuration;
    }
}
