package br.com.mapreduce;

public class Constants {
    //Commands
    static final String COMMAND_LEAST_SQUARE = "least";
    //Commands Explanation
    static final String COMMAND_EXPLANATION_LEAST_SQUARE = "least square explanation";
    //Commands arguments
    public static final String COMMAND_ARGUMENTS_LEAST_SQUARE =
            "least <input path> <output path> <work station number> <start date (yyyyMMdd)> <end date (yyyyMMdd)>";

    //FIELDs on the Set of Weather Data
    public static final String[] FIELDS = {
        "STN---",
        "WBAN",
        "YEARMODA",
        "TEMP",
        "DEWP",
        "SLP",
        "STP",
        "VISIB",
        "WDSP",
        "MXSPD",
        "GUST",
        "MAX",
        "MIN",
        "PRCP",
        "SNDP",
        "FRSHTTYEARMODA",
        "TEMP",
        "DEWP",
        "SLP",
        "STP",
        "VISIB",
        "WDSP",
        "MXSPD",
        "GUST",
        "MAX",
        "MIN",
        "PRCP",
        "SNDP",
        "FRSHTT"
    };
}
