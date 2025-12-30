package com.praktikum.database.testing.library.dao;
// Import classes untuk database operations dan model
import com.praktikum.database.testing.library.config.DatabaseConfig;
import com.praktikum.database.testing.library.model.User;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Data Access Object (DAO) class untuk entity User
 * Menangani semua operasi CRUD (Create, Read, Update, Delete) untuk tabel users
 * Menggunakan PreparedStatement untuk prevent SQL injection
 */
public class UserDAO {

    /**
     * CREATE - Insert user baru ke database
     * @param user User object yang akan dibuat (tanpa userId)
     * @return User object yang sudah dibuat (dengan userId yang di-generate)
     * @throws SQLException jika operasi database gagal
     */
    public User create(User user) throws SQLException {
        // SQL query dengan RETURNING clause untuk mendapatkan generated ID
        String sql = "INSERT INTO users (username, email, full_name, phone, role, status) " +
                "VALUES (?, ?, ?, ?, ?, ?) " +
                "RETURNING user_id, registration_date, created_at, updated_at";

        // Try-with-resources untuk auto-close connection dan prepared statement
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // Set parameter values untuk prepared statement
            pstmt.setString(1, user.getUsername());
            pstmt.setString(2, user.getEmail());
            pstmt.setString(3, user.getFullName());
            pstmt.setString(4, user.getPhone());

            // Gunakan default value jika role null
            pstmt.setString(5, user.getRole() != null ? user.getRole() : "member");

            // Gunakan default value jika status null
            pstmt.setString(6, user.getStatus() != null ? user.getStatus() : "active");

            // Execute query dan dapatkan ResultSet
            ResultSet rs = pstmt.executeQuery();

            // Process ResultSet untuk mendapatkan generated values
            if (rs.next()) {
                // Set generated values ke user object
                user.setUserId(rs.getInt("user_id"));
                user.setRegistrationDate(rs.getTimestamp("registration_date"));
                user.setCreatedAt(rs.getTimestamp("created_at"));
                user.setUpdatedAt(rs.getTimestamp("updated_at"));
            }

            return user;
        }
    }

    /**
     * READ - Mencari user berdasarkan ID
     * @param userId ID user yang dicari
     * @return Optional containing User jika ditemukan, empty Optional jika tidak
     * @throws SQLException jika operasi database gagal
     */
    public Optional<User> findById(Integer userId) throws SQLException {
        String sql = "SELECT * FROM users WHERE user_id = ?";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, userId);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                return Optional.of(mapResultSetToUser(rs));
            }
            return Optional.empty();
        }
    }

    /**
     * READ - Mencari user berdasarkan username
     * @param username Username yang dicari
     * @return Optional containing User jika ditemukan, empty Optional jika tidak
     * @throws SQLException jika operasi database gagal
     */
    public Optional<User> findByUsername(String username) throws SQLException {
        String sql = "SELECT * FROM users WHERE username = ?";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, username);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                return Optional.of(mapResultSetToUser(rs));
            }
            return Optional.empty();
        }
    }

    /**
     * READ - Mendapatkan semua users dari database
     * @return List of semua users, sorted by user_id
     * @throws SQLException jika operasi database gagal
     */
    public List<User> findAll() throws SQLException {
        String sql = "SELECT * FROM users ORDER BY user_id";
        List<User> users = new ArrayList<>();

        try (Connection conn = DatabaseConfig.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                users.add(mapResultSetToUser(rs));
            }
        }
        return users;
    }

    /**
     * UPDATE - Update data user yang sudah ada
     * @param user User object dengan data yang di-update
     * @return true jika update berhasil, false jika tidak ada user yang di-update
     * @throws SQLException jika operasi database gagal
     */
    public boolean update(User user) throws SQLException {
        String sql = "UPDATE users SET email = ?, full_name = ?, phone = ?, " +
                "role = ?, status = ?, last_login = ? WHERE user_id = ?";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, user.getEmail());
            pstmt.setString(2, user.getFullName());
            pstmt.setString(3, user.getPhone());
            pstmt.setString(4, user.getRole());
            pstmt.setString(5, user.getStatus());
            pstmt.setTimestamp(6, user.getLastLogin());
            pstmt.setInt(7, user.getUserId());

            return pstmt.executeUpdate() > 0;
        }
    }

    /**
     * UPDATE - Update last_login timestamp untuk user
     * @param userId ID user yang login
     * @return true jika update berhasil
     * @throws SQLException jika operasi database gagal
     */
    public boolean updateLastLogin(Integer userId) throws SQLException {
        String sql = "UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE user_id = ?";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, userId);
            return pstmt.executeUpdate() > 0;
        }
    }

    /**
     * DELETE - Menghapus user berdasarkan ID
     * @param userId ID user yang akan dihapus
     * @return true jika delete berhasil, false jika tidak ada user yang dihapus
     * @throws SQLException jika operasi database gagal
     */
    public boolean delete(Integer userId) throws SQLException {
        String sql = "DELETE FROM users WHERE user_id = ?";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, userId);
            return pstmt.executeUpdate() > 0;
        }
    }

    /**
     * COUNT - Menghitung total jumlah users
     * @return jumlah total users
     * @throws SQLException jika operasi database gagal
     */
    public int countAll() throws SQLException {
        String sql = "SELECT COUNT(*) FROM users";

        try (Connection conn = DatabaseConfig.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
    }

    /**
     * Helper method untuk mapping ResultSet ke User object
     * @param rs ResultSet dari database query
     * @return User object yang sudah di-mapping
     * @throws SQLException jika mapping gagal
     */
    private User mapResultSetToUser(ResultSet rs) throws SQLException {
        // Use builder pattern untuk membuat User object
        return User.builder()
                .userId(rs.getInt("user_id"))
                .username(rs.getString("username"))
                .email(rs.getString("email"))
                .fullName(rs.getString("full_name"))
                .phone(rs.getString("phone"))
                .role(rs.getString("role"))
                .status(rs.getString("status"))
                .registrationDate(rs.getTimestamp("registration_date"))
                .lastLogin(rs.getTimestamp("last_login"))
                .createdAt(rs.getTimestamp("created_at"))
                .updatedAt(rs.getTimestamp("updated_at"))
                .build();
    }
}