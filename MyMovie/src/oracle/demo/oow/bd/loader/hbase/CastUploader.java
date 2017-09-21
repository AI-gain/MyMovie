package oracle.demo.oow.bd.loader.hbase;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import oracle.demo.oow.bd.constant.Constant;
import oracle.demo.oow.bd.dao.hbase.CastDao;
import oracle.demo.oow.bd.dao.hbase.MovieDao;
import oracle.demo.oow.bd.to.CastTO;

public class CastUploader {
	
	public static void main(String args[]) throws IOException{
		CastUploader castUploader = new CastUploader();
		System.out.println("开始初始化电影角色数据**********************");
		castUploader.uploadCastInfo();
		System.out.println("开始初始化电影类型数据**********************");
	}
	
	public void uploadCastInfo() throws IOException{
		
		FileReader fr = null;
        try {
            fr = new FileReader(Constant.WIKI_MOVIE_CAST_FILE_NAME);
            BufferedReader br = new BufferedReader(fr);
            String jsonTxt = null;
            CastTO castTO = null;
            int count = 1;

            //Each line in the file is the JSON string

            //Construct MovieTO from JSON object
            while ((jsonTxt = br.readLine()) != null) {
                try {
                    castTO = new CastTO(jsonTxt.trim());
                } catch (Exception e) {
                    System.out.println("ERROR: Not able to parse the json string: \t" +
                                       jsonTxt);
                }

                if (castTO != null) {
                    System.out.println(count++ + " " + castTO.getJsonTxt());
                    /**
                     * Save the movie into the kv-store
                     */
                    //CastDAO castDAO = new CastDAO();
                    //MovieDao movieDao = new MovieDao();
                    CastDao castDao = new CastDao();

            
                    //castDAO.insertCastInfo(castTO);
                    //movieDao.insertMovieCast(castTO);
                    castDao.insertCastInfo(castTO);
                   
                    //castDAO.insertCastInfoRDBMS(castTO);

                } //EOF if

            } //EOF while
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            fr.close();
        }
	}

}
