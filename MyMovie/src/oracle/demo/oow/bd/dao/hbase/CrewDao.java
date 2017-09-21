package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
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
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.hbase.HBaseDB;

public class CrewDao {

	/**
	 * 插入电影制作相关人员信息表
	 * @param crewTO
	 * @throws IOException 
	 */
	public void insertCrewInfo(CrewTO crewTO) throws IOException {
		// TODO Auto-generated method stub
		HBaseDB hbaseDB = HBaseDB.getInstance();
		Table table = hbaseDB.getTableByName(ConstantsHBase.TABLE_CREW);
		if(table!=null){
			List<Put> puts = new ArrayList<>();
			
			//添加crew列族相关信息
			Put put_crew = new Put(Bytes.toBytes(crewTO.getStringId()));
			put_crew.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW), Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_NAME), 
					Bytes.toBytes(crewTO.getName()));
			put_crew.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW), Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_JOB), 
					Bytes.toBytes(crewTO.getJob()));
			
			puts.add(put_crew);
			
			//添加movie列族相关信息
			List<String> movieId = crewTO.getMovieList();
			Iterator<String> iter = movieId.iterator();
			while(iter.hasNext()){
				String movie = iter.next();
				String rowkey = createRowKeyOfCandM(crewTO.getId(),movie);
				Put put_movie = new Put(Bytes.toBytes(rowkey));
				put_movie.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_MOVIE_ID), 
						Bytes.toBytes(movie));
				puts.add(put_movie);
			}
			table.put(puts);
		}
		table.close();
		hbaseDB.colseHBaseConn();
	}

	/**
	 * 组装crewId_movieId行键
	 * @param crewId
	 * @param movieId
	 * @return
	 */
	private String createRowKeyOfCandM(int crewId, String movieId) {
		// TODO Auto-generated method stub
		String rowkey="";
		rowkey = crewId+"_"+movieId;
		return rowkey;
	}
	
	/**
	 * 根据crewId获取对应制作人员制作的电影
	 * @param crewId
	 * @return
	 * @throws IOException
	 */
	public List<MovieTO> getMoviesByCrew(String crewId) throws IOException{
		List<MovieTO> movieTOList = new ArrayList<>();
		HBaseDB hbaseDB = HBaseDB.getInstance();
		//根据crewId查询crew表 获取和crew相关的movieId
		List<String> movieIdList = new ArrayList<>();
		Table table = hbaseDB.getTableByName(ConstantsHBase.TABLE_CREW);
		Scan scan = new Scan();
		Filter filter = new PrefixFilter(Bytes.toBytes(crewId+"_"));
		scan.setFilter(filter);
		ResultScanner resultScanner = table.getScanner(scan);
		for (Result result : resultScanner) {
			movieIdList.add(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_MOVIE_ID))));
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
	public void testGetMoviesByCrew() throws IOException{
		
		CrewDao crewDao = new CrewDao();
		crewDao.getMoviesByCrew("1410");
	}
}
