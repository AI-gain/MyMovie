package oracle.demo.oow.bd.loader.hbase;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import oracle.demo.oow.bd.constant.Constant;
import oracle.demo.oow.bd.constant.KeyConstant;
import oracle.demo.oow.bd.dao.hbase.GenreDao;
import oracle.demo.oow.bd.dao.hbase.MovieDao;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;

public class GenreUploader {
	
	
	public static void main(String args[]) throws IOException{
		GenreUploader genreUploader = new GenreUploader();
		System.out.println("开始初始化电影类型数据**********************");
		genreUploader.uploadGenreInfo();
		System.out.println("初始化电影类型数据结束**********************");
	}
	
	/**
	 * 上传电影类型相关数据
	 * @throws IOException
	 */
	public void uploadGenreInfo() throws IOException{
		 FileReader fr = null;
	        try {
	            fr = new FileReader(Constant.WIKI_MOVIE_INFO_FILE_NAME);
	            BufferedReader br = new BufferedReader(fr);
	            String jsonTxt = null;
	            MovieTO movieTO = null;
	            //MovieDAO movieDAO = new MovieDAO();
	            //MovieDao movieDao = new MovieDao();
	            GenreDao genreDao = new GenreDao();
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
                      genreDao.insertGenreInfo(movieTO);

                     //movieDAO.insertMovieRDBMS(movieTO);

                 } //EOF if
	            } //EOF while


	        } catch (Exception e) {
	            e.printStackTrace();
	        } finally {
	           fr.close();
				
	        }
	}
}
