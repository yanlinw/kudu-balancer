package org.apache.kudu.balancer;

import java.io.IOException;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kudu.balancer.parsers.MasterParser;
import org.apache.kudu.balancer.parsers.PartitionParser;


public class Runner {
	

	public static void main(String args[]) throws ParseException, IOException {

		Options options = new Options();
		options.addOption("m", "masters", true, "kudu master address.")
		.addOption("u", "url", true, "url for any kudu master http page")
		.addOption("t", "tables", true, "table names, separate by comma")
		.addOption("o", "output", true, "output mode, choose between: move/leader/all")
		.addOption("r", "range", true, "only balance specific partitions, separate by comma");

		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("Kudu partition balancer", options);
		final CommandLineParser cmdLineParser = new DefaultParser();
		CommandLine commandLine = cmdLineParser.parse(options, args);
		String masterUrl = commandLine.getOptionValue("u");
		String masters = commandLine.getOptionValue("m");
		String leaderMaster = MasterParser.getleader(masterUrl);
		

		
		String tables[] = commandLine.getOptionValue("t").split(",");
		String ranges[] = commandLine.getOptionValue("r")==null?null:commandLine.getOptionValue("r").split(",");
		String mode = commandLine.getOptionValue("o");

		

		run (tables, mode, leaderMaster, masters, ranges);

	}
	
	
	public static void run(String [] tables, String mode, String leaderMaster,String masters, String ranges[]) throws IOException {

		PartitionParser parser = new PartitionParser(masters);
		
		
		for(String table:tables) {
			
			
			
			switch (mode) {
			case "move":
				System.out.println("Running move planning mode");
				parser.parse(leaderMaster, table, mode);
				System.out.println(parser.printMovementPlan(ranges));
				break;
			case "leader":
				System.out.println("Showing leader tablets distribution");
				parser.parse(leaderMaster, table, mode);
				System.out.println(parser.printLeaderHist(ranges));
				break;
			case "all":
				System.out.println("Showing all tablets distribution");
				parser.parse(leaderMaster, table, mode);
				System.out.println(parser.printAllHist(ranges));
				break;
			default:
				System.out.println("invalid output option");
			}
		}
	}




}
