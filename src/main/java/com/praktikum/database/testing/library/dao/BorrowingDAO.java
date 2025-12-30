package com.praktikum.database.testing.library.dao;

// Import classes untuk database operations dan model
import com.praktikum.database.testing.library.config.DatabaseConfig;
import com.praktikum.database.testing.library.model.Borrowing;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Data Access Object (DAO) class untuk entity Borrowing
 * Menangani semua operasi CRUD untuk tabel borrowings
 * Mengelola data peminjaman buku oleh users
 */
public class BorrowingDAO {

    /**
     * CREATE - Insert borrowing record baru ke database
     * @param borrowing Borrowing object yang akan dibuat
     * @return Borrowing object yang sudah dibuat dengan generated ID
     * @throws SQLException jika operasi database gagal
     */
    public Borrowing create(Borrowing borrowing) throws SQLException {
        // SQL query untuk insert borrowing record
        String sql = "INSERT INTO borrowings (user_id, book_id, due_date, status, notes) " +
                "VALUES (?, ?, ?, ?, ?) " +
                "RETURNING borrowing_id, borrow_date, created_at, updated_at";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // Set parameter values
            pstmt.setInt(1, borrowing.getUserId());
            pstmt.setInt(2, borrowing.getBookId());
            pstmt.setTimestamp(3, borrowing.getDueDate());

            // Gunakan default value jika status null
            pstmt.setString(4, borrowing.getStatus() != null ? borrowing.getStatus() : "borrowed");
            pstmt.setString(5, borrowing.getNotes());

            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                // Set generated values
                borrowing.setBorrowingId(rs.getInt("borrowing_id"));
                borrowing.setBorrowDate(rs.getTimestamp("borrow_date"));
                borrowing.setCreatedAt(rs.getTimestamp("created_at"));
                borrowing.setUpdatedAt(rs.getTimestamp("updated_at"));
            }
            return borrowing;
        }
    }

    /**
     * READ - Mencari borrowing record berdasarkan ID
     * @param borrowingId ID borrowing record yang dicari
     * @return Optional containing Borrowing jika ditemukan
     * @throws SQLException jika operasi database gagal
     */
    public Optional<Borrowing> findById(Integer borrowingId) throws SQLException {
        String sql = "SELECT * FROM borrowings WHERE borrowing_id = ?";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, borrowingId);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                return Optional.of(mapResultSetToBorrowing(rs));
            }
            return Optional.empty();
        }
    }

    /**
     * READ - Mencari semua borrowing records untuk user tertentu
     * @param userId ID user yang borrowing records-nya dicari
     * @return List of borrowing records untuk user tersebut
     * @throws SQLException jika operasi database gagal
     */
    public List<Borrowing> findByUserId(Integer userId) throws SQLException {
        String sql = "SELECT * FROM borrowings WHERE user_id = ? ORDER BY borrow_date DESC";
        List<Borrowing> borrowings = new ArrayList<>();

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, userId);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                borrowings.add(mapResultSetToBorrowing(rs));
            }
        }
        return borrowings;
    }

    /**
     * READ - Mencari semua borrowing records untuk book tertentu
     * @param bookId ID book yang borrowing records-nya dicari
     * @return List of borrowing records untuk book tersebut
     * @throws SQLException jika operasi database gagal
     */
    public List<Borrowing> findByBookId(Integer bookId) throws SQLException {
        String sql = "SELECT * FROM borrowings WHERE book_id = ? ORDER BY borrow_date DESC";
        List<Borrowing> borrowings = new ArrayList<>();

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, bookId);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                borrowings.add(mapResultSetToBorrowing(rs));
            }
        }
        return borrowings;
    }

    /**
     * READ - Mencari active borrowing records (belum dikembalikan)
     * @return List of active borrowing records
     * @throws SQLException jika operasi database gagal
     */
    public List<Borrowing> findActiveBorrowings() throws SQLException {
        String sql = "SELECT * FROM borrowings WHERE return_date IS NULL ORDER BY borrow_date DESC";
        List<Borrowing> borrowings = new ArrayList<>();

        try (Connection conn = DatabaseConfig.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                borrowings.add(mapResultSetToBorrowing(rs));
            }
        }
        return borrowings;
    }

    /**
     * READ - Mencari overdue borrowing records (melewati due_date dan belum dikembalikan)
     * @return List of overdue borrowing records
     * @throws SQLException jika operasi database gagal
     */
    public List<Borrowing> findOverdueBorrowings() throws SQLException {
        String sql = "SELECT * FROM borrowings WHERE return_date IS NULL AND due_date < CURRENT_TIMESTAMP " +
                "ORDER BY due_date ASC";
        List<Borrowing> borrowings = new ArrayList<>();

        try (Connection conn = DatabaseConfig.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                borrowings.add(mapResultSetToBorrowing(rs));
            }
        }
        return borrowings;
    }

    /**
     * UPDATE - Update borrowing record untuk menandai buku dikembalikan
     * @param borrowingId ID borrowing record yang akan di-update
     * @param returnDate Tanggal pengembalian buku
     * @return true jika update berhasil
     * @throws SQLException jika operasi database gagal
     */
    public boolean returnBook(Integer borrowingId, Timestamp returnDate) throws SQLException {
        String sql = "UPDATE borrowings SET return_date = ?, status = 'returned', updated_at = CURRENT_TIMESTAMP " +
                "WHERE borrowing_id = ? AND return_date IS NULL";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setTimestamp(1, returnDate);
            pstmt.setInt(2, borrowingId);

            return pstmt.executeUpdate() > 0;
        }
    }

    /**
     * UPDATE - Update status borrowing record
     * @param borrowingId ID borrowing record yang akan di-update
     * @param status Status baru (borrowed, returned, overdue, lost)
     * @return true jika update berhasil
     * @throws SQLException jika operasi database gagal
     */
    public boolean updateStatus(Integer borrowingId, String status) throws SQLException {
        String sql = "UPDATE borrowings SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE borrowing_id = ?";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, status);
            pstmt.setInt(2, borrowingId);

            return pstmt.executeUpdate() > 0;
        }
    }

    /**
     * UPDATE - Update fine amount untuk borrowing record
     * @param borrowingId ID borrowing record yang akan di-update
     * @param fineAmount Jumlah denda yang harus dibayar
     * @return true jika update berhasil
     * @throws SQLException jika operasi database gagal
     */
    public boolean updateFineAmount(Integer borrowingId, Double fineAmount) throws SQLException {
        String sql = "UPDATE borrowings SET fine_amount = ?, updated_at = CURRENT_TIMESTAMP WHERE borrowing_id = ?";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setDouble(1, fineAmount);
            pstmt.setInt(2, borrowingId);

            return pstmt.executeUpdate() > 0;
        }
    }

    /**
     * DELETE - Menghapus borrowing record berdasarkan ID
     * @param borrowingId ID borrowing record yang akan dihapus
     * @return true jika delete berhasil
     * @throws SQLException jika operasi database gagal
     */
    public boolean delete(Integer borrowingId) throws SQLException {
        String sql = "DELETE FROM borrowings WHERE borrowing_id = ?";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, borrowingId);
            return pstmt.executeUpdate() > 0;
        }
    }

    /**
     * COUNT - Menghitung total borrowing records
     * @return jumlah total borrowing records
     * @throws SQLException jika operasi database gagal
     */
    public int countAll() throws SQLException {
        String sql = "SELECT COUNT(*) FROM borrowings";

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
     * COUNT - Menghitung active borrowing records untuk user tertentu
     * @param userId ID user yang active borrowing-nya dihitung
     * @return jumlah active borrowing records
     * @throws SQLException jika operasi database gagal
     */
    public int countActiveBorrowingsByUser(Integer userId) throws SQLException {
        String sql = "SELECT COUNT(*) FROM borrowings WHERE user_id = ? AND return_date IS NULL";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, userId);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
    }

    /**
     * Helper method untuk mapping ResultSet ke Borrowing object
     * @param rs ResultSet dari database query
     * @return Borrowing object yang sudah di-mapping
     * @throws SQLException jika mapping gagal
     */
    private Borrowing mapResultSetToBorrowing(ResultSet rs) throws SQLException {
        return Borrowing.builder()
                .borrowingId(rs.getInt("borrowing_id"))
                .userId(rs.getInt("user_id"))
                .bookId(rs.getInt("book_id"))
                .borrowDate(rs.getTimestamp("borrow_date"))
                .dueDate(rs.getTimestamp("due_date"))
                .returnDate(rs.getTimestamp("return_date"))
                .status(rs.getString("status"))
                .fineAmount(rs.getBigDecimal("fine_amount"))
                .finePaid(rs.getBoolean("fine_paid"))
                .notes(rs.getString("notes"))
                .createdAt(rs.getTimestamp("created_at"))
                .updatedAt(rs.getTimestamp("updated_at"))
                .build();
    }
}