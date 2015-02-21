import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.ServerAddress;

import java.net.UnknownHostException;
import java.util.*;
import java.util.Map.Entry;
import java.math.*;




public class MongoDB{
	
	static MongoClient mongoClient;
	static DB db;
	static DBCollection coll;
	static DBCollection coll1;
	
	MongoDB() throws UnknownHostException {

		mongoClient = new MongoClient( "localhost" , 27017 );
		db = mongoClient.getDB("InvertedIndex");
		coll = db.getCollection("InvIndex");
	    coll1 = db.getCollection("DocMap");
	}
	
   public  void fnMongo(Map<String,  List<posObj>> wordMap){
      try{   
    	  
          int count = 0;
          
          List<posObj> docList = new ArrayList<posObj>();

          System.out.println("Mapsize: " + wordMap.size());

          Iterator iterMap = wordMap.keySet().iterator();
          while(iterMap.hasNext()){
        	  
        	  if(count%1000==0)
        		  System.out.println("Storing word " + count + " to " + count+1000);
        		  
        	  count++;

        	  String word = (String)iterMap.next();
        	  docList = wordMap.get(word);
        	  
              BasicDBObject query = new BasicDBObject();
              query.put("word", word);
              
              DBCursor cursor = coll.find(query);
              if (cursor.hasNext()) { 
            	  
                  DBObject obj = (DBObject) cursor.next();
                 
                  List<BasicDBObject> temp = (List<BasicDBObject>) obj.get("doc");

                  for(posObj tempposobj: docList){
                   	  temp.add(new BasicDBObject("id",tempposobj.getDocID()).append("pos", tempposobj.getPos()).append("frequency", tempposobj.getFreq()));
                  }
                 obj.put("doc", temp);
                 coll.save(obj); 
                 
              }
              else{
            	                 
            	  List<BasicDBObject> listObj = new ArrayList<BasicDBObject>();
            	  
                  for(posObj tempposobj: docList){
                	  listObj.add(new BasicDBObject("id",tempposobj.getDocID()).append("pos", tempposobj.getPos()).append("frequency", tempposobj.getFreq()));
                  }
                  
                  coll.insert(new BasicDBObject("doc",listObj).
                		  append("word",word));

            	  
              }

        	  
          }
          
      }catch(Exception e){
	     System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	  }
      
   }
   
   public void fnFind(String search) throws UnknownHostException{
	   
	    
        BasicDBObject query = new BasicDBObject();
        query.put("word", search);
        
        DBCursor cursor = coll.find(query);
        if (cursor.hasNext()) { 
      	  
            DBObject obj = (DBObject) cursor.next();
                      
            List<BasicDBObject> temp = (List<BasicDBObject>) obj.get("doc");

            for(BasicDBObject tempObj: temp){
            	
                BasicDBObject queryUrl = new BasicDBObject();
                queryUrl.put("docID", tempObj.get("id"));
                DBCursor cursorUrl = coll1.find(queryUrl);
                
                if (cursorUrl.hasNext()) { 
              	  
                    DBObject objUrl = (DBObject) cursorUrl.next();
                              
                	System.out.println("[" +tempObj.get("frequency")+"] " + objUrl.get("url"));
                }
                
            	
            }
     	 
        }
        else{
        	System.out.println("Sorry! Word not found");

        }
        
   }
   
   
   public void fnSearch(String search) throws UnknownHostException{
	   	   
	   String[] searchArr = search.split(" ");
	   
	   Map<String,Integer> AndOrMap = new HashMap<String, Integer>();
	   
	   for(String word: searchArr){
		   
		   BasicDBObject query = new BasicDBObject();
	       query.put("word", word);
	       DBCursor cursor = coll.find(query);
	       
	       if (cursor.hasNext()) {   
	           DBObject obj = (DBObject) cursor.next();	                     
	           List<BasicDBObject> temp = (List<BasicDBObject>) obj.get("doc");
	           
	           for(BasicDBObject tempObj: temp){
	        	   int count = 0;
	        	   String docid = tempObj.get("id").toString();
	        	   if(AndOrMap.containsKey(docid)){
	        		   count = AndOrMap.get(docid);
	        	   }
	        	   AndOrMap.put(docid, count+1);
	           }
	       } 
	   }
	   
	   if(AndOrMap.size() == 0){
		   
		   System.out.println("Not found\n");
		   return;
		   
	   }
		   
	   
	   Map<String,Integer> AndOrMapHigh = new HashMap<String, Integer>();
	   
	   	List<Entry<String, Integer>> AndOrList = new ArrayList<Entry<String, Integer>>(AndOrMap.entrySet());
	   	List<Entry<String, Integer>> AndOrListLow = new ArrayList<Entry<String, Integer>>();

		for(Map.Entry<String, Integer> mapEntry:AndOrList){

			if(mapEntry.getValue() > 1){
				
				AndOrMapHigh.put(mapEntry.getKey(), mapEntry.getValue());
			}
			else{
				AndOrListLow.add(mapEntry);
			}
	
		}
	   	

	   	List<Entry<String, Integer>> tokenPairList = new ArrayList<Entry<String, Integer>>(AndOrMapHigh.entrySet());
		Collections.sort( tokenPairList, new Comparator<Map.Entry<String, Integer>>()
		{
			public int compare( Map.Entry<String, Integer> mapEntry1, Map.Entry<String, Integer> mapEntry2 )
			{
				return (mapEntry2.getValue()).compareTo( mapEntry1.getValue() );
			}
		
		} );
		
		tokenPairList.addAll(AndOrListLow);
				
		System.out.println("High Freq Size " + AndOrMapHigh.size());
		System.out.println("Full Size " + tokenPairList.size());
		
		int size = tokenPairList.size();
		
		Scanner in = new Scanner(System.in);
		String input = "n";
		
		int current = 0;
		int limit = 0;
		int page = 1;
		
		while(input.matches("n")){
		
			System.out.println("\n-----------------------------------------------------------\n" + search + " [ Page " + page + " ]\n\n" );
			
				if(current + 20 > size)
					limit = size;
				else
					limit = current + 20;
			
				
		for(int i = current; i< limit; i++, current++){
			Map.Entry<String, Integer> mapEntry = tokenPairList.get(i);
//			System.out.println(mapEntry.getValue());
			 BasicDBObject queryUrl = new BasicDBObject();
			 queryUrl.put("docID", mapEntry.getKey());
			 DBCursor cursorUrl = coll1.find(queryUrl);
           
			 if (cursorUrl.hasNext()) { 
				 
				DBObject objUrl = (DBObject) cursorUrl.next();
                      
           		System.out.println(mapEntry.getValue() +" "+ objUrl.get("url"));
          		
           }
		}
		
		if(current>=size)
			break;
		
		System.out.println("\n\n'n' for page " + ++page);
		input = in.nextLine();
		

		}
		
		System.out.println("\n\n");
		
//	   Iterator mapIter = AndOrMap.keySet().iterator();
//	   while(mapIter.hasNext()){
//		   System.out.println(AndOrMap.get(mapIter.next()));		   
//	   } 
//	   
//       BasicDBObject query = new BasicDBObject();
//       query.put("word", search);
//       
//       DBCursor cursor = coll.find(query);
//       if (cursor.hasNext()) { 
//     	  
//           DBObject obj = (DBObject) cursor.next();
//                     
//           List<BasicDBObject> temp = (List<BasicDBObject>) obj.get("doc");
//
//           for(BasicDBObject tempObj: temp){
//           	
//               BasicDBObject queryUrl = new BasicDBObject();
//               queryUrl.put("docID", tempObj.get("id"));
//               DBCursor cursorUrl = coll1.find(queryUrl);
//               
//               if (cursorUrl.hasNext()) { 
//             	  
//                   DBObject objUrl = (DBObject) cursorUrl.next();
//                             
//               	System.out.println("[" +tempObj.get("frequency")+"] " + objUrl.get("url"));
//               }
//               
//           	
//           }
//    	 
//       }
//       else{
//       	System.out.println("Sorry! Word not found");
//
//       }
       
  }
   
   
   
   public void fnWriteDocMap(Map<String, String> docMap) throws UnknownHostException{
	   
	    Iterator itr = docMap.keySet().iterator();
	    while(itr.hasNext()){
	    	
	    	String id = (String)itr.next();
	    	String url = docMap.get(id);
	    	
            coll1.insert(new BasicDBObject("docID",id).
          		  append("url",url));

	    }
	    
   }
   
   public void fnCalculateTFIDF() throws UnknownHostException{
	   

        DBCursor cursor = coll1.find();
        int N = cursor.length();
        System.out.println("N: " + cursor.length());
	   
	    DBCollection coll = db.getCollection("InvIndex");
        DBCursor cursor1 = coll.find();
        
        while(cursor1.hasNext()){
        	
            DBObject obj = (DBObject) cursor1.next();
            
            List<BasicDBObject> temp = (List<BasicDBObject>) obj.get("doc");
            List<BasicDBObject> tempnew = new ArrayList<BasicDBObject>();
            int NT = temp.size();

            for(BasicDBObject tempInstObj: temp){
            	
            	
            	int tf = (int)tempInstObj.get("frequency");
            	
                double tfidf = (Math.log(1+tf))*(Math.log((float)N/NT));

                tempInstObj.put("tfidf", tfidf);
                tempnew.add(tempInstObj);
            	
            }
        	obj.put("doc", tempnew);
        	coll.save(obj);
        	
        }
   }
   
}