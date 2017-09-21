package oracle.demo.oow.bd.loader.hbase;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import oracle.demo.oow.bd.constant.Constant;
import oracle.demo.oow.bd.dao.hbase.CrewDao;
import oracle.demo.oow.bd.dao.hbase.MovieDao;
import oracle.demo.oow.bd.to.CrewTO;

public class CrewUploader {
	
	public static void main(String args[]) throws IOException{
		CrewUploader crewUploader = new CrewUploader();
		System.out.println("开始初始化电影制作相关数据**********************");
		crewUploader.uploadCrewInfo();
		System.out.println("开始初始化电影制作相关数据**********************");
	}
	
	public void uploadCrewInfo() throws IOException{
		
		FileReader fr = null;
        try {
            fr = new FileReader(Constant.WIKI_MOVIE_CREW_FILE_NAME);
            BufferedReader br = new BufferedReader(fr);
            String jsonTxt = null;
            CrewTO crewTO = null;
            int count = 1;

            //Each line in the file is the JSON string

            //Construct MovieTO from JSON object
            while ((jsonTxt = br.readLine()) != null) {
                try {
                    crewTO = new CrewTO(jsonTxt.trim());
                } catch (Exception e) {
                    System.out.println("ERROR: Not able to parse the json string: \t" +
                                       jsonTxt);
                }

                if (crewTO != null) {
                    System.out.println(count++ + " " + crewTO.getJsonTxt());
                    /**
                     * Save the movie into the kv-store
                     */
                    //CrewDAO crewDAO = new CrewDAO();
                  //crewDAO.insertMovieCrew(crewTO);
                    //MovieDao movieDao = new MovieDao();
                    //movieDao.insertMoiveCrew(crewTO);
                    CrewDao crewDao = new CrewDao();
                    crewDao.insertCrewInfo(crewTO);
                    
                    

//                    if (targetDatabase == Constant.TARGET_BOTH ||
//                        targetDatabase == Constant.TARGET_RDBMS)
//                        crewDAO.insertCrewInfoRDBMS(crewTO);

                } //EOF if

            } //EOF while
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            fr.close();
        }
	}
}
