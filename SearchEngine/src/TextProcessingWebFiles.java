

import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;



public class TextProcessingWebFiles{
	
     private static List<String>  list = new ArrayList<String>();

/* Method to compute tokenize the input text file */
	 
	 public static List<String> tokenizeText (String TextFilePath) throws IOException {
		
		/* creating FileReader object for the input file*/
		
		FileReader inputtextfile = new FileReader(TextFilePath);
		
		/* Creating BufferedReader obj for the input file */
		
		BufferedReader bufferread = new BufferedReader(inputtextfile);
		
		String eachline;
		
		try
		{
			while((eachline = bufferread.readLine() ) != null){
			eachline = eachline.trim();	
			eachline = eachline.toLowerCase();
			if(!( eachline.isEmpty())){
				String[] tokens = eachline.split("[^a-zA-Z0-9]+");
				for(String token : tokens){
					if(token.length()>0)
						list.add(token);
				}
			}
		  }
		}

		catch(IOException e)
		{
		     System.out.println(e);	
		}
		
		finally
		{
			try {
				
				if(bufferread != null)
					bufferread.close();
				
			}
			catch(IOException e){
				System.out.println(e);
			}
		}
		
		return list;
	}

}