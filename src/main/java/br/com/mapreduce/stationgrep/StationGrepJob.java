package br.com.mapreduce.stationgrep;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public class StationGrepJob extends Configured implements Tool{
    public static final String CONF_KEY_NUMBER = "CONF_KEY_NUMBER";
    public static final String NAME = "StationGrepJob";
    public static final int RESULT_CODE_SUCCESS = 1;

    public int run(String[] strings) throws Exception {
        return 0;
    }
}
