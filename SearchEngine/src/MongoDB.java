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
import java.math.*;




public class MongoDB{
	
	 
	
   public static void fnMongo(Map<String,  Map<String, posObj>> wordMap){
      try{   
    	  
    	  // To connect to mongodb server
    	    MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
    	    // Now connect to your databases
    	    DB db = mongoClient.getDB("InvertedIndex");
    	   // System.out.println("Connect to database successfully");
    	    DBCollection coll = db.getCollection("InvIndex");
    	   // System.out.println("Collection mycol selected successfully");
          
          Map<String, Map<String, List<Integer>>> wordList = new HashMap<String, Map<String, List<Integer>>>();
          Map<String, posObj> docList = new HashMap<String, posObj>();
          posObj pos = new posObj();
          
          Map<String, posObj> docListDB = new HashMap<String, posObj>();
          posObj posDB = new posObj();

         // System.out.println("Map" + wordMap);

          Iterator iterMap = wordMap.keySet().iterator();
          while(iterMap.hasNext()){
        	  
        	  String word = (String)iterMap.next();
        	  docList = wordMap.get(word);
        	  
        	  Iterator iterMap1 = docList.keySet().iterator();
        	  while(iterMap1.hasNext()){
        		  
        		  String docID = (String)iterMap1.next();
        		  pos = docList.get(docID);        		  
        		  //System.out.println(pos.position);
        		  //System.out.println(pos.freq);
        		  //Check DB and add/update
        		  
                  BasicDBObject query = new BasicDBObject();
                  query.put("word", word);
                  
                  DBCursor cursor = coll.find(query);
                  if (cursor.hasNext()) { 
                	  
                      DBObject obj = (DBObject) cursor.next();
                      String strDocID = docID.toString();
                      
                    // System.out.println(obj.get("word")); 
                    // System.out.println(obj.get("doc"));
                     
                     //docListDB = (Map<String, posObj>) obj.get("doc");
                     
                      List<BasicDBObject> temp = (List<BasicDBObject>) obj.get("doc");

               	  temp.add(new BasicDBObject("docInst",new BasicDBObject("id",docID).append("pos", pos.getPos()).append("frequency", pos.getFreq())));
                     obj.put("doc", temp);
                     coll.save(obj); 
                     
                  }
                  else{
                	  
                     // System.out.println("Create");
                	  List<BasicDBObject> listObj = new ArrayList<BasicDBObject>();
                	  listObj.add(new BasicDBObject("docInst",new BasicDBObject("id",docID).append("pos", pos.getPos()).append("frequency", pos.getFreq())));
                	  
                      coll.insert(new BasicDBObject("doc",listObj).
                    		  append("word",word));

                	  
                  }

        		  
        	  }
        	  
          }
          
      }catch(Exception e){
	     System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	  }
   }
   
   public static void fnFind(String search) throws UnknownHostException{
	   
	   // To connect to mongodb server
	    MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
	    // Now connect to your databases
	    DB db = mongoClient.getDB("InvertedIndex");
	   // System.out.println("Connect to database successfully");
	    DBCollection coll = db.getCollection("InvIndex");
	    DBCollection coll1 = db.getCollection("DocMap");

	   // System.out.println("Collection mycol selected successfully");
	    
        BasicDBObject query = new BasicDBObject();
        query.put("word", search);
        
        DBCursor cursor = coll.find(query);
        if (cursor.hasNext()) { 
      	  
            DBObject obj = (DBObject) cursor.next();
                      
            List<BasicDBObject> temp = (List<BasicDBObject>) obj.get("doc");

            for(BasicDBObject tempObj: temp){
            	
            	tempObj = (BasicDBObject)tempObj.get("docInst");
            	
            	
                BasicDBObject queryUrl = new BasicDBObject();
                queryUrl.put("docID", tempObj.get("id"));
                DBCursor cursorUrl = coll1.find(queryUrl);
                
                if (cursorUrl.hasNext()) { 
              	  
                    DBObject objUrl = (DBObject) cursorUrl.next();
                              
                	System.out.println("Document: " + objUrl.get("url"));
                }
                
            	
            }
     	 
        }
        else{
        	System.out.println("Sorry! Word not found");

        }
   }
   
   
   public static void fnWriteDocMap(Map<String, String> docMap) throws UnknownHostException{
	   
	   // To connect to mongodb server
	    MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
	    // Now connect to your databases
	    DB db = mongoClient.getDB("InvertedIndex");
	   // System.out.println("Connect to database successfully");
	    DBCollection coll = db.getCollection("DocMap");
	   // System.out.println("Collection mycol selected successfully");

	    Iterator itr = docMap.keySet().iterator();
	    while(itr.hasNext()){
	    	
	    	String id = (String)itr.next();
	    	String url = docMap.get(id);
	    	
            coll.insert(new BasicDBObject("docID",id).
          		  append("url",url));

	    }
	   
   }
   
   public static void fnCalculateTFIDF() throws UnknownHostException{
	   
	   // To connect to mongodb server
	    MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
	    // Now connect to your databases
	    DB db = mongoClient.getDB("InvertedIndex");
	   // System.out.println("Connect to database successfully");
	    DBCollection coll = db.getCollection("DocMap");
	   // System.out.println("Collection mycol selected successfully");

        DBCursor cursor = coll.find();
        int N = cursor.length();
        System.out.println("N: " + cursor.length());
	   
	    DBCollection coll1 = db.getCollection("InvIndex");
        DBCursor cursor1 = coll1.find();
        
        while(cursor1.hasNext()){
        	
            DBObject obj = (DBObject) cursor1.next();
            
            List<BasicDBObject> temp = (List<BasicDBObject>) obj.get("doc");

            int NT = temp.size();
            
            for(BasicDBObject tempObj: temp){
            	
            	BasicDBObject tempInstObj = (BasicDBObject)tempObj.get("docInst");
            	
            	int tf = (int)tempInstObj.get("frequency");
            	
            	double tfidf = (Math.log(1+tf))*(Math.log(N/NT));
            	
            	tempInstObj.put("tfidf", tfidf);
            	tempObj.put("docInst", tempInstObj);
            	obj.put("doc", temp);
            	coll1.save(obj);
            	
            }

        	
        }

        
   }
   
}