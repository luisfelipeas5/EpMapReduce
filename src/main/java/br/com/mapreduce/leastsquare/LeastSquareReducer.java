package br.com.mapreduce.leastsquare;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class LeastSquareReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        String measurement = context.getConfiguration().get(LeastSquareJob.CONF_NAME_MEASUREMENT);
        if (key.equals( new Text(measurement))) {
            for (DoubleWritable value : values) {
                //TODO least square caculation
                System.out.println(value.toString());
            }
            context.write(key, result);
        }
    }
}
