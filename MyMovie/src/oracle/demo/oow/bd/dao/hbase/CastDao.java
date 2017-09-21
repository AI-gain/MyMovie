package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
import oracle.demo.oow.bd.to.CastMovieTO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.hbase.HBaseDB;

public class CastDao {

	/**
	 * 插入电影角色表相关信息
	 * @param castTO
	 * @throws IOException 
	 */
	public void insertCastInfo(CastTO castTO) throws IOException{
		// TODO Auto-generated method stub
		HBaseDB hbaseDB = HBaseDB.getInstance();
		Table table = hbaseDB.getTableByName(ConstantsHBase.TABLE_CAST);
		if(table!=null){
			List<Put> puts = new ArrayList<>();
			//插入cast列族相关信息
			Put put_cast = new Put(Bytes.toBytes(castTO.getStringId()));
			put_cast.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_CAST), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_NAME), 
					Bytes.toBytes(castTO.getName()));
			puts.add(put_cast);
			//插入movie列族相关信息
			List<CastMovieTO> castMovieTOs = castTO.getCastMovieList();
			Iterator<CastMovieTO> iter = castMovieTOs.iterator();
			while(iter.hasNext()){
				CastMovieTO castMovieTO = iter.next();
				Put put_movie = new Put(Bytes.toBytes(createRowKeyOfCandM(castTO.getId(),castMovieTO.getId())));
				put_movie.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_MOVIE_ID), 
						Bytes.toBytes(castMovieTO.getId()));
				put_movie.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_CHARACTER), 
						Bytes.toBytes(castMovieTO.getCharacter()));
				put_movie.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_ORDER), 
						Bytes.toBytes(castMovieTO.getOrder()));
				puts.add(put_movie);
			}
			table.put(puts);
		}
		table.close();
		hbaseDB.colseHBaseConn();
		
	}

	/**
	 * 组装castId_movieId行键
	 * @param castId
	 * @param movieId
	 * @return
	 */
	private String createRowKeyOfCandM(int castId,int movieId) {
		// TODO Auto-generated method stub
		String rowkey="";
		rowkey = castId +"_" +movieId;
		return rowkey;
	}
	
	/**
	 * 根据castId获取对应演员参演的电影
	 * @param castId
	 * @return
	 * @throws IOException
	 */
	public List<MovieTO> getMoviesByCast(String castId) throws IOException{
		List<MovieTO> movieTOList = new ArrayList<>();
		HBaseDB hbaseDB = HBaseDB.getInstance();
		//根据castId查询cast表 获取和cast相关的movieId
		List<String> movieIdList = new ArrayList<>();
		Table table = hbaseDB.getTableByName(ConstantsHBase.TABLE_CAST);
		Scan scan = new Scan();
		Filter filter = new PrefixFilter(Bytes.toBytes(castId+"_"));
		scan.setFilter(filter);
		ResultScanner resultScanner = table.getScanner(scan);
		for (Result result : resultScanner) {
			movieIdList.add(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_MOVIE_ID)))+"");
		}
		//根据查询到的movieId获取movie详细信息
		if(movieIdList.size()!=0){
			System.out.println(movieIdList);
			Iterator<String> iter = movieIdList.iterator();
			while(iter.hasNext()){
				MovieDao movieDao = new MovieDao();
				MovieTO movieTO = movieDao.getMovieById(iter.next());
				movieTOList.add(movieTO);
			}
		}
		table.close();
		hbaseDB.colseHBaseConn();
		return movieTOList;
	}

	@Test
	public void testGetMoviesByCast() throws IOException{
		CastDao castDao = new CastDao();
		castDao.getMoviesByCast("8495");
	}
}
