package db.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import db.DBConnection;
import entity.Item;
import entity.Item.ItemBuilder;
import external.TicketMasterAPI;

/**
 * @author terrance_cw
 * 
 */
public class MySQLConnection implements DBConnection {

	private Connection connection;

	public MySQLConnection() {
		try {
			Class.forName("com.mysql.cj.jdbc.Driver").getConstructor().newInstance();
			connection = DriverManager.getConnection(MySQLDBUtil.URL);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		if (connection != null) {
			try {
				connection.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void setFavoriteItems(String userId, List<String> itemIds) {
		String sql = "INSERT IGNORE INTO history(user_id, item_id) VALUES (?, ?)";
		updateFavoriteItems(sql, userId, itemIds);
	}

	@Override
	public void unsetFavoriteItems(String userId, List<String> itemIds) {
		String sql = "DELETE FROM history WHERE user_id = ? AND item_id = ?";
		updateFavoriteItems(sql, userId, itemIds);
	}

	private void updateFavoriteItems(String sql, String userId, List<String> itemIds) {
		if (connection == null) {
			System.err.println("DB connection failed");
			return;
		}

		try {
			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setString(1, userId);
			for (String itemId : itemIds) {
				ps.setString(2, itemId);
				ps.execute();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public Set<String> getFavoriteItemIds(String userId) {
		if (connection == null) {
			return new HashSet<>();
		}

		Set<String> favoriteItems = new HashSet<>();
		try {
			String sql = "SELECT item_id FROM history WHERE user_id = ?";
			PreparedStatement statement = connection.prepareStatement(sql);
			statement.setString(1, userId);
			ResultSet results = statement.executeQuery();

			while (results.next()) {
				String itemId = results.getString("item_id");
				favoriteItems.add(itemId);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return favoriteItems;
	}

	@Override
	public Set<Item> getFavoriteItems(String userId) {
		if (connection == null) {
			return new HashSet<>();
		}

		Set<Item> favoriteItems = new HashSet<>();
		Set<String> itemIds = getFavoriteItemIds(userId);
		try {
			String sql = "SELECT * FROM items WHERE item_id = ?";
			PreparedStatement statement = connection.prepareStatement(sql);
			for (String itemId : itemIds) {
				statement.setString(1, itemId);
				ResultSet results = statement.executeQuery();
				ItemBuilder builder = new ItemBuilder();

				while (results.next()) {
					builder.setItemId(results.getString("item_id"));
					builder.setName(results.getString("name"));
					builder.setAddress(results.getString("address"));
					builder.setImageUrl(results.getString("image_url"));
					builder.setUrl(results.getString("url"));
					builder.setCategories(getCategories(itemId));
					builder.setDistance(results.getDouble("distance"));
					builder.setRating(results.getDouble("rating"));

					favoriteItems.add(builder.build());
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return favoriteItems;
	}

	@Override
	public Set<String> getCategories(String itemId) {
		if (connection == null) {
			return new HashSet<>();
		}

		Set<String> categories = new HashSet<>();
		try {
			String sql = "SELECT category from categories WHERE item_id = ? ";
			PreparedStatement statement = connection.prepareStatement(sql);
			statement.setString(1, itemId);
			ResultSet results = statement.executeQuery();

			while (results.next()) {
				String category = results.getString("category");
				categories.add(category);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return categories;
	}

	@Override
	public List<Item> searchItems(double lat, double lon, String term) {
		TicketMasterAPI ticketMasterAPI = new TicketMasterAPI();
		List<Item> items = ticketMasterAPI.search(lat, lon, term);

		for (Item item : items) {
			saveItem(item);
		}

		return items;
	}

	@Override
	public void saveItem(Item item) {
		if (connection == null) {
			System.err.println("DB connection failed");
			return;
		}

		// sql injection
		// select * from users where username = '' AND password = '';

		// username: fakeuser ' OR 1 = 1; DROP --
		// select * from users where username = 'fakeuser ' OR 1 = 1 --' AND password =
		// '';

		try {
			String sql = "INSERT IGNORE INTO items VALUES (?, ?, ?, ?, ?, ?, ?)";
			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setString(1, item.getItemId());
			ps.setString(2, item.getName());
			ps.setDouble(3, item.getRating());
			ps.setString(4, item.getAddress());
			ps.setString(5, item.getImageUrl());
			ps.setString(6, item.getUrl());
			ps.setDouble(7, item.getDistance());
			ps.execute();

			sql = "INSERT IGNORE INTO categories VALUES(?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setString(1, item.getItemId());
			for (String category : item.getCategories()) {
				ps.setString(2, category);
				ps.execute();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public String getFullname(String userId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean verifyLogin(String userId, String password) {
		// TODO Auto-generated method stub
		return false;
	}

}
