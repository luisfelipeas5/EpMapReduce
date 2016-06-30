package br.com.mapreduce;

public class Constants {
    //Commands
    static final String COMMAND_LEAST_SQUARE = "least";
    static final String COMMAND_MEAN = "mean";
    //Commands Explanation
    static final String COMMAND_EXPLANATION_LEAST_SQUARE = "least square explanation";
    static final String COMMAND_EXPLANATION_MEAN = "mean explanation";

    //Commands arguments
    public static final String COMMAND_ARGUMENTS_LEAST_SQUARE = "least <input path> <output path> <work station number> <start date (yyyyMMdd)> <end date (yyyyMMdd)> <measure> <x>";
    public static final String COMMAND_ARGUMENTS_STATION_GREP = "station <input path> <output path> <work station number>";
    public static final String COMMAND_ARGUMENTS_DATE_GREP = "date <input path> <output path> <start date (yyyyMMdd)> <end date (yyyyMMdd)>";
    public static final String COMMAND_ARGUMENTS_MEAN = "mean <input path> <output path> <work station number> <start date (yyyyMMdd)> <end date (yyyyMMdd)> <measure>";
    public static final String COMMAND_ARGUMENTS_STD_DEVIATION = "deviation <input path> <output path> <work station number> <start date (yyyyMMdd)> <end date (yyyyMMdd)>";

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
