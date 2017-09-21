package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.hbase.DBUtil;

public class CustomerRatingDao {
	
	/**
	 * 添加用户对电影的评价
	 * @param userId
	 * @param movieId
	 * @param rating
	 */
	public void insertCustomerRating(int userId,int movieId,int rating){
		
		String insert = null;
		PreparedStatement stmt =null;
		Connection conn = DBUtil.getConn();
		
		insert = "INSERT INTO CUST_RATING (USERID, MOVIEID, RATING)  VALUES (?, ?, ?)";
		try {
			if(conn!=null){
				stmt = conn.prepareStatement(insert);
				stmt.setInt(1, userId);
				stmt.setInt(2, movieId);
				stmt.setInt(3, rating);
				stmt.execute();
				DBUtil.close(stmt, conn);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
	}
	
	/**
	 * 根据用户名删除用户对相关信息的评价
	 * @param userId
	 */
	public void deleteCustomerRating(int userId){
		String delete = null;
		PreparedStatement stmt =null;
		Connection conn = DBUtil.getConn();
		
		delete = "DELETE FROM CUST_RATING WHERE USERID = ?";
		try {
			if(conn!=null){
				stmt = conn.prepareStatement(delete);
				stmt.setInt(1, userId);
				stmt.execute();
				DBUtil.close(stmt, conn);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	public List<MovieTO> getMoviesByMood(int custId) throws IOException{
		List<MovieTO> movieTOList = new ArrayList<>();
		String get = null;
		PreparedStatement stmt =null;
		Connection conn = DBUtil.getConn();
		
		get = "SELECT movieId FROM cust_mood WHERE USERID = ? ORDER BY RATING DESC";
		List<String> movieIdList = new ArrayList<>();
		try {
			if(conn!=null){
				stmt = conn.prepareStatement(get);
				stmt.setInt(1, custId);
				ResultSet rs = stmt.executeQuery();
				while(rs.next()){
					movieIdList.add(rs.getString(1));
				}
				System.out.println(movieIdList);
				if(movieIdList.size()!=0){
					Iterator<String> iter = movieIdList.iterator();
					MovieDao movieDao = new MovieDao();
					while(iter.hasNext()){
						MovieTO movieTO = movieDao.getMovieById(iter.next());
						if(movieTO!=null){
							movieTOList.add(movieTO);
						}
					}
					if(movieTOList.size()==0){
						movieTOList = null;
					}
				}
				else
				{
					movieTOList = null;
				}
				DBUtil.close(rs, stmt, conn);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return movieTOList;
	}
	
	@Test
	public void testInsert(){
		CustomerRatingDao cDao = new CustomerRatingDao();
		cDao.insertCustomerRating(1, 2, 3);
	}
	
	@Test
	public void testDelete(){
		CustomerRatingDao cDao = new CustomerRatingDao();
		cDao.deleteCustomerRating(1);
	}
	
	@Test
	public void testGet() throws IOException{
		CustomerRatingDao cDao = new CustomerRatingDao();
		cDao.getMoviesByMood(1255601);
	}
}
