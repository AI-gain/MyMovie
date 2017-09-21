package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import oracle.demo.oow.bd.constant.hbase.ConstantsHBase;
import oracle.demo.oow.bd.to.CastCrewTO;
import oracle.demo.oow.bd.to.CastMovieTO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.hbase.HBaseDB;

public class MovieDao {

	/**
	 * 添加电影基本信息和分类信息
	 * @param movieTO
	 */
	public void insertMovieInfoAndGenres(MovieTO movieTO) {
		// TODO Auto-generated method stub
		
		HBaseDB hbaseDB = HBaseDB.getInstance();
		Table table = hbaseDB.getTableByName(ConstantsHBase.TABLE_MOVIE);
		
		if(table!=null){
			List<Put> puts = new ArrayList<>();
			//电影基本信息
			Put put_movie = new Put(Bytes.toBytes(movieTO.getStringId()));
			//original_title
			put_movie.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_ORIGINAL_TITLE),
					Bytes.toBytes(movieTO.getTitle()));
			//overview
			put_movie.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_OVERVIEW),
					Bytes.toBytes(movieTO.getOverview()));
			//poster_path
			put_movie.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POSTER_PATH),
					Bytes.toBytes(movieTO.getPosterPath()));
			//release_date
			put_movie.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RELEASE_DATE),
					Bytes.toBytes(movieTO.getReleasedYear()));
			//vote_count
			put_movie.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_VOTE_COUNT),
					Bytes.toBytes(movieTO.getVoteCount()));
			//runtime
			put_movie.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RUNTIME),
					Bytes.toBytes(movieTO.getRunTime()));
			//popularity
			put_movie.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POPULARITY),
					Bytes.toBytes(movieTO.getPopularity()));
			
			puts.add(put_movie);
			
			//电影类型
			ArrayList<GenreTO> genreList = movieTO.getGenres();
			Iterator<GenreTO> iter = genreList.iterator();
            while (iter.hasNext()) {
                 GenreTO genreTO = iter.next();
                 Put put_genre = new Put(Bytes.toBytes(createRowkeyofMandG(movieTO,genreTO)));
                 put_genre.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_GENRE_ID),
                		 Bytes.toBytes(genreTO.getId()));
                 put_genre.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_GENRE_NAME),
                		 Bytes.toBytes(genreTO.getName()));
                 puts.add(put_genre);
            }
			
			try {
				table.put(puts);
				table.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			hbaseDB.colseHBaseConn();
		}
	}

	/**
	 * 添加电影角色信息
	 * @param castTO
	 * @throws IOException 
	 */
	public void insertMovieCast(CastTO castTO) throws IOException {
		// TODO Auto-generated method stub
		
		HBaseDB hbaseDB = HBaseDB.getInstance();
		Table table = hbaseDB.getTableByName(ConstantsHBase.TABLE_MOVIE);
		if(table!=null){
			List<Put> puts = new ArrayList<>();
			List<String> rowKeys = createRowKeyofMandCast(castTO);
			Iterator<String> iter = rowKeys.iterator();
			while(iter.hasNext()){
				String rowkey = iter.next();
				Put put = new Put(Bytes.toBytes(rowkey));
				put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CAST), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_CAST_ID),
						Bytes.toBytes(castTO.getId()));
				puts.add(put);
			}
			table.put(puts);
		}
		table.close();
		hbaseDB.colseHBaseConn();
	}

	/**
	 * 添加电影制作相关人员信息
	 * @param crewTO
	 * @throws IOException 
	 */
	public void insertMoiveCrew(CrewTO crewTO) throws IOException {
		// TODO Auto-generated method stub
		HBaseDB hbaseDB = HBaseDB.getInstance();
		Table table = hbaseDB.getTableByName(ConstantsHBase.TABLE_MOVIE);
		if(table!=null){
			List<Put> puts = new ArrayList<>();

			List<String> rowKeys = createRowKeyofMandCrew(crewTO);
			Iterator<String> iter = rowKeys.iterator();
			while(iter.hasNext()){
				String rowkey = iter.next();
				Put put = new Put(Bytes.toBytes(rowkey));
				
				put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CREW), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_CREW_ID), 
						Bytes.toBytes(crewTO.getId()));
				puts.add(put);
			}
			
			table.put(puts);
		}
		table.close();
		hbaseDB.colseHBaseConn();
	}
	
	/**
	 * 组装行键movieId_genreId
	 * @param movieTO
	 * @param genreTO
	 * @return
	 */
	private String createRowkeyofMandG(MovieTO movieTO, GenreTO genreTO) {
		// TODO Auto-generated method stub
		String rowKey = "";
		rowKey = movieTO.getId()+"_"+genreTO.getId();
		return rowKey;
	}

	/**
	 * 根据movieId返回movie信息
	 * @param movieId
	 * @return
	 * @throws IOException 
	 */
	public MovieTO getMovieById(String movieId) throws IOException{
		
		//定义电影类型信息
		ArrayList<GenreTO> genres = new ArrayList<>();
		//定义电影演员id列表
		List<Integer> castId = new ArrayList<>();
		//定义电影制作人员id列表
		List<Integer> crewId = new ArrayList<>();
		
		MovieTO movieTO = new MovieTO();
		HBaseDB hbaseDB = HBaseDB.getInstance();
		Table table_movie = hbaseDB.getTableByName(ConstantsHBase.TABLE_MOVIE);
//		Scan scan = new Scan();
//		Filter filter = new PrefixFilter(Bytes.toBytes(movieId));
//		scan.setFilter(filter);
//		ResultScanner resultScanner = table_movie.getScanner(scan);
		Get get = new Get(Bytes.toBytes(movieId));
		Result result_movie = table_movie.get(get);
		
		//获取movie列族信息(基本信息)
		byte[] title = result_movie.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_ORIGINAL_TITLE));
		byte[] overView = result_movie.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_OVERVIEW));
		byte[] posterPath = result_movie.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POSTER_PATH));
		byte[] date = result_movie.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RELEASE_DATE));
		byte[] voteCount = result_movie.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_VOTE_COUNT));
		byte[] runTime = result_movie.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RUNTIME));
		byte[] popularity = result_movie.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POPULARITY));
		
		if(title!=null){
			//System.out.println(Integer.parseInt(Bytes.toString(result.getRow())));
			movieTO.setId(Integer.parseInt(Bytes.toString(result_movie.getRow())));
			movieTO.setTitle(Bytes.toString(title));
		}
		if(overView!=null){
			movieTO.setOverview(Bytes.toString(overView));
		}
		if(posterPath!=null){
			movieTO.setPosterPath(Bytes.toString(posterPath));
		}
		if(date!=null){
			movieTO.setDate(Bytes.toInt(date)+"");
		}
		if(voteCount!=null){
			movieTO.setVoteCount(Bytes.toInt(voteCount));
		}
		if(runTime!=null){
			movieTO.setRunTime(Bytes.toInt(runTime));
		}
		if(popularity!=null){
			movieTO.setPopularity(Bytes.toDouble(popularity));
		}
			
		//通过前缀过滤器过滤movie表的genre、cast、crew信息
		Scan scan = new Scan();
		Filter filter = new PrefixFilter(Bytes.toBytes(movieId));
		scan.setFilter(filter);
		ResultScanner resultScanner = table_movie.getScanner(scan);
		for (Result result : resultScanner) {
			//获取genre列族信息(类型)
			byte[] genreId = result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_GENRE_ID));
			byte[] genreName = result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_GENRE_NAME));
			
			GenreTO genre = new GenreTO();
			if(genreId!=null){
				genre.setId(Bytes.toInt(genreId));
			}
			if(genreName!=null){
				genre.setName(Bytes.toString(genreName));
				genres.add(genre);
			}
			
			
			//获取cast列族信息(castId)
			byte[] cast_Id = result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CAST), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_CAST_ID));
			if(cast_Id!=null){
				castId.add(Bytes.toInt(cast_Id));
			}
			//获取crew列族信息(crewId)
			byte[] crew_Id = result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CREW), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_CREW_ID));
			if(crew_Id!=null){
				crewId.add(Bytes.toInt(crew_Id));
			}
		}
		//获取电影类型相关信息
		movieTO.setGenres(genres);
		
		//获取演员和电影制作人员信息
		CastCrewTO castCrewTO = new CastCrewTO();
		//获取电影演员信息
		Table table_cast = hbaseDB.getTableByName(ConstantsHBase.TABLE_CAST);
		Iterator<Integer> iterCast = castId.iterator();
		while(iterCast.hasNext()){
			Get get_cast = new Get(Bytes.toBytes(iterCast.next().toString()));
			Result result = table_cast.get(get_cast);
			CastTO castTO = new CastTO();
			castTO.setId(Integer.parseInt(Bytes.toString(result.getRow())));
			castTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_CAST),Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_NAME))));
			castCrewTO.addCastTO(castTO);
		}
		table_cast.close();
		
		//获取电影制作人员相关信息
		Table table_crew = hbaseDB.getTableByName(ConstantsHBase.TABLE_CREW);
		Iterator<Integer> iterCrew = crewId.iterator();
		while(iterCrew.hasNext()){
			Get get_crew = new Get(Bytes.toBytes(iterCrew.next().toString()));
			Result result = table_crew.get(get_crew);
			CrewTO crewTO = new CrewTO();
			crewTO.setId(Integer.parseInt(Bytes.toString(result.getRow())));
			crewTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW), Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_NAME))));
			crewTO.setJob(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW), Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_JOB))));
			castCrewTO.addCrewTO(crewTO);
		}
		table_crew.close();
		
		movieTO.setCastCrewTO(castCrewTO);
		resultScanner.close();
		table_movie.close();
		hbaseDB.colseHBaseConn();
		return movieTO;
	}
	
	/**
	 * 组装行键moiveId_castId
	 * @param castTO
	 * @return 行键列表
	 */
	private List<String> createRowKeyofMandCast(CastTO castTO) {
		// TODO Auto-generated method stub
		List<String> rowKeys = new ArrayList<>();
		List<CastMovieTO> castMovieTOs = castTO.getCastMovieList();
		Iterator<CastMovieTO> iter = castMovieTOs.iterator();
		while(iter.hasNext()){
			CastMovieTO castMovieTO = iter.next();
			rowKeys.add(castMovieTO.getStringId()+"_"+castTO.getId());
		}
		return rowKeys;
	}

	/**
	 * 组装movieId_crewId
	 * @param crewTO
	 * @return 行键列表
	 */
	private List<String> createRowKeyofMandCrew(CrewTO crewTO) {
		// TODO Auto-generated method stub
		List<String> rowKeys = new ArrayList<>();
		List<String> movielist = crewTO.getMovieList();
		Iterator<String> iter = movielist.iterator();
		while(iter.hasNext()){
			String rowkey = iter.next()+"_"+crewTO.getId();
			rowKeys.add(rowkey);
		}
		return rowKeys;
	}
	
	@Test
	public void testGetMovieById() throws IOException{
		MovieDao movieDao = new MovieDao();
		MovieTO movieTO = movieDao.getMovieById("180");
		System.out.println(movieTO.getTitle());
		CastCrewTO castCrewTO = movieTO.getCastCrewTO();
		
		List<CastTO> castTo = castCrewTO.getCastList();
		Iterator<CastTO> iter_cast = castTo.iterator();
		while(iter_cast.hasNext()){
			System.out.print(iter_cast.next().getName());
		}
		System.out.println("");
		List<CrewTO> crewTo = castCrewTO.getCrewList();
		Iterator<CrewTO> iter_crew = crewTo.iterator();
		while(iter_crew.hasNext()){
			System.out.print(iter_crew.next().getJob());
		}
	}
}
