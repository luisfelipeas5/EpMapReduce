package br.com.mapreduce.dategrep;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public class DateGrepJob extends Configured implements Tool {
    public static final String NAME = "DateGrepJob";
    public static final int RESULT_CODE_SUCCESS = 1;

    public int run(String[] strings) throws Exception {
        return 0;
    }
}
