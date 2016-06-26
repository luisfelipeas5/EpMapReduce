package br.com.mapreduce.leastsquare;

import br.com.mapreduce.Constants;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class LeastSquareMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("key = " + key.toString());
        System.out.println(value.toString());

        //TODO map as KEY = FIELD
        int tokenIndex = 0;
        DoubleWritable tokenValue = new DoubleWritable();
        Text tokenKey = new Text(Constants.FIELDS[tokenIndex]);
        context.write(tokenKey, tokenValue);
    }
}
