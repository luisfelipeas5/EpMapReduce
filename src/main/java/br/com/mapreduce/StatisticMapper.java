package br.com.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class StatisticMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
        for (int tokenIndex = 0; tokenIndex < stringTokenizer.countTokens(); tokenIndex++) {
            String token = stringTokenizer.nextToken();
            try {
                double parseDouble = Double.parseDouble(token);
                DoubleWritable tokenValue = new DoubleWritable(parseDouble);
                Text tokenKey = new Text(Constants.FIELDS[tokenIndex]);
                System.out.println("<" + tokenKey + ", " + tokenValue + ">");
                context.write(tokenKey, tokenValue);
            } catch (NumberFormatException numberFormatException) {
                return;
            }
        }
    }
}
