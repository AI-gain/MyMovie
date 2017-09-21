package oracle.demo.oow.bd.loader.hbase;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import oracle.demo.oow.bd.constant.Constant;
import oracle.demo.oow.bd.constant.KeyConstant;
import oracle.demo.oow.bd.dao.CastDAO;
import oracle.demo.oow.bd.dao.CrewDAO;
import oracle.demo.oow.bd.dao.hbase.MovieDao;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;

public class MovieUploader {
	
	
	public static void main(String args[]) throws IOException{
		MovieUploader movieUploader = new MovieUploader();
		System.out.println("开始初始化电影基本信息和所属类型数据**********************");
		movieUploader.uploadMoiveInfoAndGenre();
		System.out.println("开始初始化电影基本信息和所属类型数据**********************");
		System.out.println("开始初始化电影演员数据**********************");
		movieUploader.uploadMovieCast();
		System.out.println("初始化电影演员数据结束**********************");
		System.out.println("开始初始化电影制作人员数据*********************");
		movieUploader.uploadMovieCrew();
		System.out.println("初始化电影制作人员数据结束*********************");
	}
	
	/**
	 * 上传电影基本信息和类型信息
	 * @throws IOException 
	 */
	public void uploadMoiveInfoAndGenre() throws IOException{
		
		  FileReader fr = null;
	        try {
	            fr = new FileReader(Constant.WIKI_MOVIE_INFO_FILE_NAME);
	            BufferedReader br = new BufferedReader(fr);
	            String jsonTxt = null;
	            MovieTO movieTO = null;
	            //MovieDAO movieDAO = new MovieDAO();
	            MovieDao movieDao = new MovieDao();
	            int count = 1;

	            //Each line in the file is the JSON string

	            //Construct MovieTO from JSON object
	            while ((jsonTxt = br.readLine()) != null) {

//	                if (maxMovies <= 0 || (maxMovies > 0 && count < maxMovies)) {
//	                    
//	                } //if(maxMovies>0 && count < maxMovies)
	            	try {
                        movieTO = new MovieTO(jsonTxt.trim());
                    } catch (Exception e) {
                        System.out.println("ERROR: Not able to parse the json string: \t" +
                                           jsonTxt);
                    }

                    if (movieTO != null && !movieTO.isAdult()) {
                        
                        System.out.println(count++ + " " +
                                           movieTO.getMovieJsonTxt());
                        
                       ArrayList<GenreTO> genreList = movieTO.getGenres();
                       ArrayList<GenreTO> genreList2 = new ArrayList<GenreTO>();
                       Iterator<GenreTO> iter = genreList.iterator();
                       while (iter.hasNext()) {
                            GenreTO genreTO = iter.next();
                            genreTO.setCid(KeyConstant.GENRE_TABLE);
                            genreList2.add(genreTO);
                            
                        }
                        movieTO.setGenres(genreList2);
                        /**
                     * Save the movie into the kv-store or rdbms
                     */

                
                        //movieDAO.insertMovie(movieTO);
                         movieDao.insertMovieInfoAndGenres(movieTO);

                        //movieDAO.insertMovieRDBMS(movieTO);

                    } //EOF if
	            } //EOF while


	        } catch (Exception e) {
	            e.printStackTrace();
	        } finally {
	           fr.close();
				
	        }

	}
	
	/**
	 * 上传电影角色信息
	 * @throws IOException
	 */
	public void uploadMovieCast() throws IOException{
		
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
                    MovieDao movieDao = new MovieDao();

            
                    //castDAO.insertCastInfo(castTO);
                    movieDao.insertMovieCast(castTO);

                   
                    //castDAO.insertCastInfoRDBMS(castTO);

                } //EOF if

            } //EOF while
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            fr.close();
        }
		
	}
	
	/**
	 * 上传电影相关制作人员信息
	 * @throws IOException
	 */
	public void uploadMovieCrew() throws IOException{
		
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
                    MovieDao movieDao = new MovieDao();
                    movieDao.insertMoiveCrew(crewTO);
                    
                    

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
