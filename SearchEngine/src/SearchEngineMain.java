/*
 * Author: Jwala Mohith Girisha
 * Student ID: 12647996
 * 
 * This is the main class. 
 * 
 * Phase 1:
 * In this class a TextProcessing object is created and used to process a file by tokenizing it and computing the Two Grams and Palindromes in it.
 * 
 */

import java.util.Date;
import java.util.Scanner;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;


public class SearchEngineMain{
	
	public static void main(String[] args) throws Exception{
		
		Scanner in = new Scanner(System.in);
	
		System.out.println("Search Engine Implementation");
	
		System.out.println("\nPart1: Text Processing\n-----------------------------------------------------------------\n");
		
		//TextProcessing txtProcObj = new TextProcessing();
		//txtProcObj.fnBeginTextProcessing();
		
		System.out.println("\nPart2: Web Crawling\n-----------------------------------------------------------------\n");

		 String crawlStorageFolder = "./Files/Dump";
         int numberOfCrawlers = 2;

         CrawlConfig config = new CrawlConfig();
         config.setCrawlStorageFolder(crawlStorageFolder);
         config.setUserAgentString("testCrawler");
         config.setPolitenessDelay(300);

         /*
          * Instantiate the controller for this crawl.
          */
         PageFetcher pageFetcher = new PageFetcher(config);
         RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
         RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
         CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

         
         /*
          * For each crawl, you need to add some seed urls. These are the first
          * URLs that are fetched and then the crawler starts following links
          * which are found in these pages
          */
         controller.addSeed("http://www.ics.uci.edu/~lopes");
         //controller.addSeed("http://www.ics.uci.edu/~lopes/teaching");
         
         
         /*
          * Start the crawl. This is a blocking operation, meaning that your code
          * will reach the line after this only when crawling is finished.
          */
         
         System.out.println(config.toString());
         
         Date dt1 = new Date();
         controller.start(MyCrawler.class, numberOfCrawlers); 
         Date dt2 = new Date();
         
			long diff = dt2.getTime() - dt1.getTime();
			long diffSeconds = diff / 1000 % 60;
			long diffMinutes = diff / (60 * 1000) % 60;
			long diffHours = diff / (60 * 60 * 1000) % 24;

         System.out.println("Time: " + diffHours + " hr " + diffMinutes + " m " + diffSeconds + " s");

         MyCrawler.writeIndexMap();
         MyCrawler.writeDomainMap();
         
         controller.shutdown();
         
         // Text processing begins
         
         
         
         System.out.println("End");

	}
	
}