package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import db.DB;
import db.DbException;
import model.dao.DepartmentDao;
import model.entities.Department;

public class DepartmentDaoJDBC implements DepartmentDao{

	private Connection conn;
	
	public DepartmentDaoJDBC(Connection conn) {
		this.conn = conn;
	}
	
	@Override
	public void insert(Department obj) {
		PreparedStatement dt = null;
		try {
			dt = conn.prepareStatement(
					"INSERT INTO department "
					+ "(Name) "
					+ "VALUES "
					+ "(?)",
					Statement.RETURN_GENERATED_KEYS);
			
			dt.setString(1, obj.getName());
			
			int rowsAffected = dt.executeUpdate();
			
			if(rowsAffected > 0) {
				ResultSet rs = dt.getGeneratedKeys();
				if(rs.next()) {
					int id = rs.getInt(1);
					obj.setId(id);
				}
				DB.closeResultSet(rs);
			}
			else {
				throw new DbException("Unexpected error! No rows affected!");
			}
		}
		catch (SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(dt);
		}
	}

	@Override
	public void update(Department obj) {
		PreparedStatement dt = null;
		try {
			dt = conn.prepareStatement(
					"UPDATE department "
					+ "SET Name = ? "
					+ "WHERE Id = ?");
			
			dt.setString(1, obj.getName());
			dt.setInt(2, obj.getId());
			
			dt.executeUpdate();
		}
		catch (SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(dt);
		}
	}

	@Override
	public void deleteById(Integer id) {
		PreparedStatement dt = null;
		try {
			dt = conn.prepareStatement("DELETE FROM department WHERE Id = ?");
			
			dt.setInt(1, id);
			
			dt.executeUpdate();
			
		}
		catch (SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(dt);
		}
	}

	@Override
	public Department findById(Integer id) {
		PreparedStatement dt = null;
		ResultSet rs = null;
		try {
			dt = conn.prepareStatement(
					"SELECT * FROM department WHERE Id = ?");
			
			dt.setInt(1, id);
			rs = dt.executeQuery();
			if(rs.next()) {
				Department obj = new Department();
				obj.setId(rs.getInt("Id"));
				obj.setName(rs.getString("Name"));
				return obj;
			}
			return null;
		}
		catch (SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(dt);
			DB.closeResultSet(rs);
		}
	}

	@Override
	public List<Department> findAll() {
		PreparedStatement dt = null;
		ResultSet rs = null;
		try {
			dt = conn.prepareStatement(
					"SELECT * FROM department ORDER BY Name");
			rs = dt.executeQuery();
			
			List<Department> list = new ArrayList<>();
			
			while (rs.next()) {
				Department obj = new Department();
				obj.setId(rs.getInt("Id"));
				obj.setName(rs.getString("Name"));
				list.add(obj);
			}
			return list;
		}
		catch (SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(dt);
			DB.closeResultSet(rs);
		}
	}

}
