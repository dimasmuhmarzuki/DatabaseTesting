package com.praktikum.database.testing.library.dao;
// Import classes untuk database operations dan model
import com.praktikum.database.testing.library.config.DatabaseConfig;
import com.praktikum.database.testing.library.model.Book;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Data Access Object (DAO) class untuk entity Book
 * Menangani semua operasi CRUD untuk tabel books
 */
public class BookDAO {

    /**
     * CREATE - Insert book baru ke database
     * @param book Book object yang akan dibuat
     * @return Book object yang sudah dibuat dengan generated ID
     * @throws SQLException jika operasi database gagal
     */
    public Book create(Book book) throws SQLException {
        // SQL query dengan banyak parameters untuk book data
        String sql = "INSERT INTO books (isbn, title, author_id, publisher_id, category_id, " +
                "publication_year, pages, language, description, total_copies, " +
                "available_copies, price, location, status) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "RETURNING book_id, created_at, updated_at";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // Set semua parameter values
            pstmt.setString(1, book.getIsbn());
            pstmt.setString(2, book.getTitle());
            pstmt.setInt(3, book.getAuthorId());

            // Handle nullable fields dengan setObject
            pstmt.setObject(4, book.getPublisherId());
            pstmt.setObject(5, book.getCategoryId());
            pstmt.setObject(6, book.getPublicationYear());
            pstmt.setObject(7, book.getPages());

            pstmt.setString(8, book.getLanguage());
            pstmt.setString(9, book.getDescription());
            pstmt.setInt(10, book.getTotalCopies());
            pstmt.setInt(11, book.getAvailableCopies());
            pstmt.setBigDecimal(12, book.getPrice());
            pstmt.setString(13, book.getLocation());

            // Gunakan default value jika status null
            pstmt.setString(14, book.getStatus() != null ? book.getStatus() : "available");

            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                // Set generated values
                book.setBookId(rs.getInt("book_id"));
                book.setCreatedAt(rs.getTimestamp("created_at"));
                book.setUpdatedAt(rs.getTimestamp("updated_at"));
            }
            return book;
        }
    }

    /**
     * READ - Mencari book berdasarkan ID
     */
    public Optional<Book> findById(Integer bookId) throws SQLException {
        String sql = "SELECT * FROM books WHERE book_id = ?";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, bookId);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                return Optional.of(mapResultSetToBook(rs));
            }
            return Optional.empty();
        }
    }

    /**
     * READ - Mencari book berdasarkan ISBN
     */
    public Optional<Book> findByIsbn(String isbn) throws SQLException {
        String sql = "SELECT * FROM books WHERE isbn = ?";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, isbn);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                return Optional.of(mapResultSetToBook(rs));
            }
            return Optional.empty();
        }
    }

    /**
     * READ - Mendapatkan semua books dari database
     */
    public List<Book> findAll() throws SQLException {
        String sql = "SELECT * FROM books ORDER BY book_id";
        List<Book> books = new ArrayList<>();

        try (Connection conn = DatabaseConfig.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                books.add(mapResultSetToBook(rs));
            }
        }
        return books;
    }

    /**
     * SEARCH - Mencari books berdasarkan title (case-insensitive)
     */
    public List<Book> searchByTitle(String title) throws SQLException {
        String sql = "SELECT * FROM books WHERE LOWER(title) LIKE LOWER(?) ORDER BY title";
        List<Book> books = new ArrayList<>();

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // Use wildcard untuk partial matching
            pstmt.setString(1, "%" + title + "%");
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                books.add(mapResultSetToBook(rs));
            }
        }
        return books;
    }

    /**
     * FIND - Mencari available books (available_copies > 0)
     */
    public List<Book> findAvailableBooks() throws SQLException {
        String sql = "SELECT * FROM books WHERE available_copies > 0 ORDER BY title";
        List<Book> books = new ArrayList<>();

        try (Connection conn = DatabaseConfig.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                books.add(mapResultSetToBook(rs));
            }
        }
        return books;
    }

    /**
     * UPDATE - Update available copies untuk book
     */
    public boolean updateAvailableCopies(Integer bookId, Integer newAvailableCopies) throws SQLException {
        String sql = "UPDATE books SET available_copies = ? WHERE book_id = ?";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, newAvailableCopies);
            pstmt.setInt(2, bookId);

            return pstmt.executeUpdate() > 0;
        }
    }
    /**
     * UPDATE - Update data book secara lengkap
     */
    public boolean update(Book book) throws SQLException {
        String sql = "UPDATE books SET isbn = ?, title = ?, author_id = ?, publisher_id = ?, " +
                "category_id = ?, publication_year = ?, pages = ?, language = ?, " +
                "description = ?, total_copies = ?, available_copies = ?, price = ?, " +
                "location = ?, status = ?, updated_at = CURRENT_TIMESTAMP " +
                "WHERE book_id = ?";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, book.getIsbn());
            pstmt.setString(2, book.getTitle());
            pstmt.setInt(3, book.getAuthorId());
            pstmt.setObject(4, book.getPublisherId());
            pstmt.setObject(5, book.getCategoryId());
            pstmt.setObject(6, book.getPublicationYear());
            pstmt.setObject(7, book.getPages());
            pstmt.setString(8, book.getLanguage());
            pstmt.setString(9, book.getDescription());
            pstmt.setInt(10, book.getTotalCopies());
            pstmt.setInt(11, book.getAvailableCopies());
            pstmt.setBigDecimal(12, book.getPrice());
            pstmt.setString(13, book.getLocation());
            pstmt.setString(14, book.getStatus());
            pstmt.setInt(15, book.getBookId());

            return pstmt.executeUpdate() > 0;
        }
    }

    /**
     * UPDATE - Decrease available copies (untuk peminjaman)
     */
    public boolean decreaseAvailableCopies(Integer bookId) throws SQLException {
        String sql = "UPDATE books SET available_copies = available_copies - 1 " +
                "WHERE book_id = ? AND available_copies > 0";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, bookId);
            return pstmt.executeUpdate() > 0;
        }
    }

    /**
     * UPDATE - Increase available copies (untuk pengembalian)
     */
    public boolean increaseAvailableCopies(Integer bookId) throws SQLException {
        String sql = "UPDATE books SET available_copies = available_copies + 1 " +
                "WHERE book_id = ? AND available_copies < total_copies";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, bookId);
            return pstmt.executeUpdate() > 0;
        }
    }

    /**
     * DELETE - Menghapus book berdasarkan ID
     */
    public boolean delete(Integer bookId) throws SQLException {
        String sql = "DELETE FROM books WHERE book_id = ?";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, bookId);
            return pstmt.executeUpdate() > 0;
        }
    }

    /**
     * COUNT - Menghitung total jumlah books
     */
    public int countAll() throws SQLException {
        String sql = "SELECT COUNT(*) FROM books";

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
     * COUNT - Menghitung jumlah available books
     */
    public int countAvailableBooks() throws SQLException {
        String sql = "SELECT COUNT(*) FROM books WHERE available_copies > 0";

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
     * Helper method untuk mapping ResultSet ke Book object
     */
    private Book mapResultSetToBook(ResultSet rs) throws SQLException {
        return Book.builder()
                .bookId(rs.getInt("book_id"))
                .isbn(rs.getString("isbn"))
                .title(rs.getString("title"))
                .authorId(rs.getInt("author_id"))
                .publisherId((Integer) rs.getObject("publisher_id"))
                .categoryId((Integer) rs.getObject("category_id"))
                .publicationYear((Integer) rs.getObject("publication_year"))
                .pages((Integer) rs.getObject("pages"))
                .language(rs.getString("language"))
                .description(rs.getString("description"))
                .totalCopies(rs.getInt("total_copies"))
                .availableCopies(rs.getInt("available_copies"))
                .price(rs.getBigDecimal("price"))
                .location(rs.getString("location"))
                .status(rs.getString("status"))
                .createdAt(rs.getTimestamp("created_at"))
                .updatedAt(rs.getTimestamp("updated_at"))
                .build();
    }
}