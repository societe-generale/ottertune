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
        // db type
        String dbtype = input.get("database_type");
        System.out.println(dbtype);
        DBCollector collector = null;
        // parameters for creating a collector.
        String username = input.get("username");
        String password = input.get("password");
        String dbURL = input.get("database_url");
        // uploader
        String uploadCode = input.get("upload_code");
        String uploadURL = input.get("upload_url");
        // workload
        String workloadName = input.get("workload_name");

        switch (dbtype) {
            case "postgres":
                collector = new PostgresCollector(dbURL, username, password);
                break;
            case "mysql":
                collector = new MySQLCollector(dbURL, username, password);
                break;
            default:
                throw new MalformedParametersException("invalid database type");
        }
        String outputDir = dbtype;
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
            summary.put("workload_name", workloadName);

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
