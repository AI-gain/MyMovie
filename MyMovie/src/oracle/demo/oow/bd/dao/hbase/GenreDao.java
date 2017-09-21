package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.constant.hbase.ConstantsHBase;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.hbase.HBaseDB;

public class GenreDao {

	/**
	 * 插入电影类型信息
	 * @param movieTO
	 * @throws IOException 
	 */
	public void insertGenreInfo(MovieTO movieTO) throws IOException {
		// TODO Auto-generated method stub
		HBaseDB hbaseDB = HBaseDB.getInstance();
		
		Table table = hbaseDB.getTableByName(ConstantsHBase.TABLE_GENRE);
		if(table!=null){
			List<Put> puts = new ArrayList<>();
			
			List<GenreTO> genrelist = movieTO.getGenres();
			Iterator<GenreTO> iter = genrelist.iterator();
			while(iter.hasNext()){
				GenreTO genre = iter.next();
				//添加genre列族信息
				if(!IsGenreExists(genre.getStringId())){
					Put put_genre = new Put(Bytes.toBytes(genre.getStringId()));
					
					put_genre.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_NAME), 
							Bytes.toBytes(genre.getName()));
					puts.add(put_genre);
				}
				//添加movie列族信息
				Put put_movie = new Put(Bytes.toBytes(createRowKeyOfGandM(genre.getId(),movieTO.getId())));
				put_movie.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_MOVIE_ID), 
						Bytes.toBytes(movieTO.getId()));
				puts.add(put_movie);
			}
			table.put(puts);
		}
		table.close();
		hbaseDB.colseHBaseConn();
		
	}

	/**
	 * 组装genreId_movieId行键
	 * @param genreId
	 * @param movieId
	 * @return
	 */
	private String createRowKeyOfGandM(int genreId, int movieId) {
		// TODO Auto-generated method stub
		String rowKey = "";
		rowKey = genreId+"_"+movieId;
		return rowKey;
	}

	/**
	 * 判断电影类型信息是否存在
	 * @param id
	 * @return
	 * @throws IOException
	 */
	private boolean IsGenreExists(String genreId) throws IOException {
		// TODO Auto-generated method stub
		boolean isExists = false;
		HBaseDB hbaseDB = HBaseDB.getInstance();
		Table table = hbaseDB.getTableByName(ConstantsHBase.TABLE_GENRE);
		
		Get get = new Get(Bytes.toBytes(genreId));
		Result result = table.get(get);
		if(result.value()!=null){
			isExists = true;
		}
		System.out.println(isExists);
		hbaseDB.colseHBaseConn();
		return isExists;
	}

	
}
