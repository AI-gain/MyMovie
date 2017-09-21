package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import oracle.demo.oow.bd.constant.hbase.ConstantsHBase;
import oracle.demo.oow.bd.pojo.ActivityType;
import oracle.demo.oow.bd.pojo.RatingType;
import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.CustomerGenreTO;
import oracle.demo.oow.bd.to.CustomerTO;
import oracle.demo.oow.bd.to.GenreMovieTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.to.ScoredGenreTO;
import oracle.demo.oow.bd.util.KeyUtil;
import oracle.demo.oow.bd.util.StringUtil;
import oracle.demo.oow.bd.util.hbase.HBaseDB;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;

public class CustomerDao {
	
	private static int MOVIE_MAX_COUNT = 25;
	private static int GENRE_MAX_COUNT = 10;
	
	//修改user的info和id列族
	public void Insert(CustomerTO custTo){
		
		HBaseDB hbaseDB = HBaseDB.getInstance();
		Table table = hbaseDB.getTableByName(ConstantsHBase.TABLE_CUSTOMER);
		if(table!=null){
			Put put = new Put(Bytes.toBytes(custTo.getUserName()));
			//username --> id 的映射
			put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CUSTOMER_ID),Bytes.toBytes(ConstantsHBase.QUALIFIER_CUSTOMER_ID),Bytes.toBytes(custTo.getId()));
			//用户基本信息
			Put put2 = new Put(Bytes.toBytes(custTo.getStringId()));
			put2.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CUSTOMER_CUSTOMER), Bytes.toBytes(ConstantsHBase.QUALIFIER_CUSTOMER_NAME), Bytes.toBytes(custTo.getName()));
			put2.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CUSTOMER_CUSTOMER), Bytes.toBytes(ConstantsHBase.QUALIFIER_CUSTOMER_USERNAME), Bytes.toBytes(custTo.getUserName()));
			put2.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CUSTOMER_CUSTOMER), Bytes.toBytes(ConstantsHBase.QUALIFIER_CUSTOMER_PASSWORD), Bytes.toBytes(custTo.getPassword()));
			put2.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CUSTOMER_CUSTOMER), Bytes.toBytes(ConstantsHBase.QUALIFIER_CUSTOMER_EMAIL), Bytes.toBytes(custTo.getEmail()));
			
			List<Put> puts = new ArrayList<>();	
			puts.add(put);
			puts.add(put2);
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

	public CustomerTO getCustomerByCredential(String username, String password) throws IOException {
		// TODO Auto-generated method stub
		CustomerTO custTo = null;
		
		//首先根据username查询id
		int id = getIdByUserName(username);
		//然后根据id查询用户基本信息
		if(id>0){
			custTo = getInfoById(id);
			System.out.println(custTo.getPassword().equals(password));
			if(custTo!=null){
				if(!custTo.getPassword().equals(password))
				{
					custTo=null;
				}
			}
		}
		System.out.println(custTo.getPassword().equals(password));
		return custTo;
	}

	private CustomerTO getInfoById(int id) {
		// TODO Auto-generated method stub
		CustomerTO custTo = new CustomerTO();
		HBaseDB hbaseDB = HBaseDB.getInstance();
		Table table = hbaseDB.getTableByName(ConstantsHBase.TABLE_CUSTOMER);
		
		Get get = new Get(Bytes.toBytes(id+""));
		
		try {
			Result result = table.get(get);
			custTo.setId(id);
			custTo.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CUSTOMER_CUSTOMER),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_CUSTOMER_NAME))));
			custTo.setUserName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CUSTOMER_CUSTOMER),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_CUSTOMER_USERNAME))));
			custTo.setPassword(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CUSTOMER_CUSTOMER),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_CUSTOMER_PASSWORD))));
			custTo.setEmail(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CUSTOMER_CUSTOMER),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_CUSTOMER_EMAIL))));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return custTo;
	}

	/**
	 * 通过 username--> id 的映射去查询id
	 * @param username
	 * @return id
	 * @throws IOException 
	 */
	public int getIdByUserName(String username) throws IOException {
		// TODO Auto-generated method stub
		HBaseDB hbaseDB = HBaseDB.getInstance();
		Table table = hbaseDB.getTableByName(ConstantsHBase.TABLE_CUSTOMER);
		
		Get get = new Get(Bytes.toBytes(username));
		int id = 0;
		
		Result result = table.get(get);
		id = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CUSTOMER_ID),Bytes.toBytes(ConstantsHBase.QUALIFIER_CUSTOMER_ID)));
	
		table.close();
		hbaseDB.colseHBaseConn();
		return id;
	}
	
	/**
	 * 查询当前用户电影类型列表
	 * @param custId
	 * @param movieMaxCount
	 * @param genreMaxCount
	 * @return
	 * @throws IOException
	 */
	public List<GenreTO> getMoviesForCustomer( int custId, int movieMaxCount, int genreMaxCount) throws IOException{
		 List<GenreTO> genreList = new ArrayList<GenreTO>();
		 //genreMaxCount = GENRE_MAX_COUNT;
		 HBaseDB hbaseDB = HBaseDB.getInstance();
		 Table table = hbaseDB.getTableByName(ConstantsHBase.TABLE_GENRE);
		 if(table!=null){
			 Scan scan = new Scan();
			 FilterList filterList = new FilterList();
			 //初始化列族过滤器
			 Filter familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_GENRE)));
			 //初始化分页过滤器
			 Filter pageFilter = new PageFilter(genreMaxCount);
			 filterList.addFilter(familyFilter);
			 filterList.addFilter(pageFilter);
			 scan.setFilter(filterList);
			 ResultScanner resultScanner = table.getScanner(scan);
			 for (Result result : resultScanner) {
				 GenreTO genreTO = new GenreTO();
				 int genreId = Integer.parseInt(Bytes.toString(result.getRow()));
				 String genreName = Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_NAME)));
				 genreTO.setId(genreId);
				 genreTO.setName(genreName);
				 genreList.add(genreTO);
			}
		 }
		 table.close();
		 hbaseDB.colseHBaseConn();
	     return genreList;
	}
	
	/**
	 * 根据用户名和电影类型返回电影信息
	 * @param custId
	 * @param genreName
	 * @return
	 * @throws IOException 
	 */
	public List<MovieTO> getMoviesForCustomerByGenre(int custId,int genreId) throws IOException{
		 return getMoviesForCustomerByGenre(custId, genreId, MOVIE_MAX_COUNT);
	}
	
	/**
	 * 根据用户id和电影类型返回电影列表
	 * @param custId
	 * @param genreId
	 * @param maxCount
	 * @return
	 * @throws IOException 
	 */
	public List<MovieTO> getMoviesForCustomerByGenre(int custId, int genreId,
			int maxCount) throws IOException {
		// TODO Auto-generated method stub
		List<MovieTO> movieList = new ArrayList<MovieTO>();
		HBaseDB hbaseDB = HBaseDB.getInstance();
		//先从Genre表中根据前缀过滤器和分页过滤器找出要显示的电影的id
		List<Integer> movieIdList = new ArrayList<>();
		
		Table table_genre =  hbaseDB.getTableByName(ConstantsHBase.TABLE_GENRE);
		Scan scan = new Scan();
		FilterList filterList = new FilterList();
		//初始化前缀过滤器
		Filter prefixFilter = new PrefixFilter(Bytes.toBytes(genreId+"_"));
		//初始化分页过滤器
		Filter pageFilter = new PageFilter(maxCount);
		
		filterList.addFilter(prefixFilter);
		filterList.addFilter(pageFilter);
		scan.setFilter(filterList);
		//获取扫描结果
		ResultScanner resultScanner = table_genre.getScanner(scan);
		for (Result result : resultScanner) {
			movieIdList.add(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_MOVIE_ID))));
		}
		resultScanner.close();
		table_genre.close();
		System.out.println(movieIdList);
		//再从Movie表中扫描电影信息
		Table table_movie = hbaseDB.getTableByName(ConstantsHBase.TABLE_MOVIE);
		Iterator<Integer> iter = movieIdList.iterator();
		while(iter.hasNext()){
			Integer movieId = iter.next();
			Get get = new Get(Bytes.toBytes(movieId.toString()));
			Result result = table_movie.get(get);
			MovieTO movieTO = new MovieTO();
			movieTO.setId(Integer.parseInt((Bytes.toString(result.getRow()))));
			movieTO.setTitle(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_ORIGINAL_TITLE))));
			movieTO.setOverview(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_OVERVIEW))));
			movieTO.setPosterPath(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POSTER_PATH))));
			movieTO.setDate(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RELEASE_DATE))));
			movieTO.setVoteCount(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_VOTE_COUNT))));
			movieTO.setRunTime(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RUNTIME))));
			movieTO.setPopularity(Bytes.toDouble(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POPULARITY))));
			movieList.add(movieTO);
		}
		table_movie.close();
		hbaseDB.colseHBaseConn();
		return movieList;
	}

	/**
	 * 通过用户Id和电影Id获取最新的用户评价信息
	 * @param custId
	 * @param movieId
	 * @return ActivityTO
	 * @throws IOException 
	 */
	public ActivityTO getMovieRating(String custId,String movieId) throws IOException{
		//定义ActivityTO对象
		ActivityTO activityTO = new ActivityTO();
		
		HBaseDB hbaseDB = HBaseDB.getInstance();
		Table table = hbaseDB.getTableByName(ConstantsHBase.TABLE_ACTIVITY);
		//从activity表查询最新的满足custId和movieId的用户活动Id
		String activityId = null;
		Scan scan = new Scan();
		//定义过滤器
		FilterList filterList = new FilterList();
		//定义单值过滤器custFilter
		Filter custFilter = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_CUSTOMER_ID),
				CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(Integer.parseInt(custId))));
		//定义单值过滤器movieFilter
		Filter movieFilter = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID),
				CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(Integer.parseInt(movieId))));
		//定义单值过滤器activtyFilter
		Filter activityFilter = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY),
				CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(ActivityType.RATE_MOVIE.getValue())));
		//定义分页过滤器
		Filter pageFilter = new PageFilter(1);
		
		//将过滤器添加到过滤器列表
		filterList.addFilter(activityFilter);
		filterList.addFilter(custFilter);
		filterList.addFilter(movieFilter);
		//filterList.addFilter(pageFilter);
		
		scan.setFilter(filterList);
		ResultScanner resultScanner = table.getScanner(scan);
		for (Result result : resultScanner) {
			System.out.print(result);
			activityId = Bytes.toString(result.getRow());
		}
		System.out.println(activityId);
		if(activityId!=null){
			//通过查询到的activityId查询用户评分
			Get get = new Get(Bytes.toBytes(activityId));
			Result result = table.get(get);
			int rate = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RATING)));
			System.out.println(rate);
			switch(rate){
				case 1:
					activityTO.setRating(RatingType.ONE);
					break;
				case 2:
					activityTO.setRating(RatingType.TWO);
					break;
				case 3:
					activityTO.setRating(RatingType.THREE);
					break;
				case 4:
					activityTO.setRating(RatingType.FOUR);
					break;
				case 5:
					activityTO.setRating(RatingType.FIVE);
					break;	
			}
		}
		else
		{
			activityTO = null;
		}
		//System.out.println(activityTO.getRating().getValue());
		table.close();
		hbaseDB.colseHBaseConn();
		return activityTO;
	}
	
	/**
	 * 测试
	 * @throws IOException
	 */
	@Test
	public void testGetMoviesForCustomer() throws IOException{
		CustomerDao custDao = new CustomerDao();
		List<GenreTO> genrelist = custDao.getMoviesForCustomer(13131, 30, 6);
		Iterator<GenreTO> iter = genrelist.iterator();
		while(iter.hasNext()){
			GenreTO genreTO = iter.next();
			System.out.println("id:"+genreTO.getId()+"  name:"+genreTO.getName());
		}
	}
	
	/**
	 * 测试
	 * @throws IOException
	 */
	@Test
	public void testGetMoviesForCustomerByGenre() throws IOException{
		CustomerDao custDao = new CustomerDao();
		List<MovieTO> movieList = custDao.getMoviesForCustomerByGenre(1255601, 1, 3);
		Iterator<MovieTO> iter = movieList.iterator();
		while(iter.hasNext()){
			MovieTO movieTO = iter.next();
			System.out.println(movieTO.getTitle());
		}
	}
	
	@Test
	public void testGetMovieRating() throws IOException{
		
		CustomerDao custDao = new CustomerDao();
		custDao.getMovieRating("1255601", "11439");
	}
}
