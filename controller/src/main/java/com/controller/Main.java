package com.controller;

import com.controller.collectors.DBCollector;
import com.controller.collectors.MySQLCollector;
import com.controller.collectors.PostgresCollector;
import com.controller.util.JSONUtil;
import org.apache.commons.cli.*;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.MalformedParametersException;
import java.util.HashMap;

/**
 * Controller main.
 * @author Shuli
 */
public class Main {
    private static final int DEFAULT_TIME = 300;  //default observation time: 300 s
    private static final int TO_MILLISECONDS = 1000;
    public static void main(String[] args) {
        int time = DEFAULT_TIME; // set time to default
        String outputDirName = "output"; // default output directory
        CommandLineParser parser = new PosixParser();
        Options options = new Options();
        options.addOption("t", "time", true, "experiment time");
        options.addOption("c", "config", true, "config file path");
        options.addOption("o", "output", true, "output directory name");
        String configFilePath = null;
        try {
            CommandLine argsLine = parser.parse(options, args);
            // parse time
            if(argsLine.hasOption("t")) {
                time = Integer.parseInt(argsLine.getOptionValue("t"));
            }
            if(argsLine.hasOption("o")) {
                outputDirName = argsLine.getOptionValue("o");
            }

            // parse config file
            if(!argsLine.hasOption("c")) {
                throw new MalformedParametersException("lack config file");
            }
            else {
                configFilePath = argsLine.getOptionValue("c");
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

        // Parse input config file
        HashMap<String, String> input = ConfigFileParser.getInputFromConfigFile(configFilePath);
        ControllerConfiguration controllerConfiguration = new ControllerConfiguration(DatabaseType.get(input.get("database_type")),
                input.get("username"), input.get("password"), input.get("database_url"), input.get("upload_code"), input.get("upload_url"),
                input.get("workload_name"));

        DBCollector collector = null;
        switch (controllerConfiguration.getDbtype()) {
            case POSTGRES:
                collector = new PostgresCollector(controllerConfiguration.getDatabaseUrl(), controllerConfiguration.getUsername(), controllerConfiguration.getPassword());
                break;
            case MYSQL:
                collector = new MySQLCollector(controllerConfiguration.getDatabaseUrl(), controllerConfiguration.getUsername(), controllerConfiguration.getPassword());
                break;
            default:
                throw new MalformedParametersException("invalid database type");
        }
        String outputDir = input.get("database_type");
        String dbtype = input.get("database_type");

        new File(outputDirName).mkdir();
        new File(outputDirName + "/" + outputDir).mkdir();

        try {
            // summary json obj
            JSONObject summary = new JSONObject();
            summary.put("observation_time", time);
            summary.put("database_type", dbtype);
            summary.put("database_version", collector.collectVersion());

            // first collection (before queries)
            PrintWriter metricsWriter = new PrintWriter(outputDirName + "/" + outputDir+ "/metrics_before.json", "UTF-8");
            metricsWriter.println(collector.collectMetrics());
            metricsWriter.flush();
            metricsWriter.close();
            PrintWriter knobsWriter = new PrintWriter(outputDirName + "/"+ outputDir + "/knobs.json", "UTF-8");
            knobsWriter.println(collector.collectParameters());
            knobsWriter.flush();
            knobsWriter.close();

            // record start time
            summary.put("start_time", System.currentTimeMillis());

            // go to sleep
            Thread.sleep(time * TO_MILLISECONDS);

            // record end time
            summary.put("end_time", System.currentTimeMillis());

            // record workload_name
            summary.put("workload_name", controllerConfiguration.getWorkloadName());

            // write summary JSONObject into a JSON file
            PrintWriter summaryout = new PrintWriter(outputDirName + "/" + outputDir + "/summary.json","UTF-8");
            summaryout.println(JSONUtil.format(summary.toString()));
            summaryout.flush();

            // second collection (after queries)
            PrintWriter metricsWriterFinal = new PrintWriter(outputDirName + "/" + outputDir + "/metrics_after.json", "UTF-8");
            metricsWriterFinal.println(collector.collectMetrics());
            metricsWriterFinal.flush();
            metricsWriterFinal.close();
        } catch (FileNotFoundException | UnsupportedEncodingException | InterruptedException e) {
            e.printStackTrace();
        }

//        Map<String, String> outfiles = new HashMap<>();
//        outfiles.put("knobs", "output/" + outputDir + "/knobs.json");
//        outfiles.put("metrics_before", "output/"+ outputDir + "/metrics_before.json");
//        outfiles.put("metrics_after", "output/"+outputDir+"metrics_after.json");
//        outfiles.put("summary", "output/"+outputDir+"summary.json");
//        ResultUploader.upload(uploadURL, uploadCode, outfiles);

    }
}
