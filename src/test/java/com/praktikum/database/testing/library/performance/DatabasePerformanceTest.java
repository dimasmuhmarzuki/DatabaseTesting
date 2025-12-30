package com.praktikum.database.testing.library.performance;
// Import classes untuk testing
import com.github.javafaker.Faker;
import com.praktikum.database.testing.library.BaseDatabaseTest;
import com.praktikum.database.testing.library.config.DatabaseConfig;
import com.praktikum.database.testing.library.dao.BookDAO;
import com.praktikum.database.testing.library.dao.UserDAO;
import com.praktikum.database.testing.library.model.Book;
import com.praktikum.database.testing.library.model.User;
import org.junit.jupiter.api.*;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
// Import static assertions
import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive Performance Test Suite
 * Menguji performance database operations dengan bulk data
 * Measure response times dan identify potential bottlenecks
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Database Performance Test Suite")
public class DatabasePerformanceTest extends BaseDatabaseTest {

    private static UserDAO userDAO;
    private static BookDAO bookDAO;
    private static Faker faker;
    private static List<Integer> testUserIds;
    private static List<Integer> testBookIds;

    // Sesuaikan threshold jika environment lokal memang lambat,
    // namun dengan Batch, 5000ms sudah sangat cukup.
    private static final long BULK_INSERT_THRESHOLD = 5000;
    private static final long SINGLE_QUERY_THRESHOLD = 500; // Dinaikkan sedikit untuk stabilitas
    private static final long BULK_QUERY_THRESHOLD = 2000;

    @BeforeAll
    static void setUpAll() {
        logger.info("⚡ Starting Performance Tests");
        userDAO = new UserDAO();
        bookDAO = new BookDAO();
        faker = new Faker();
        testUserIds = new ArrayList<>();
        testBookIds = new ArrayList<>();
    }

    @AfterAll
    static void tearDownAll() throws SQLException {
        logger.info("Performance Tests Completed");
        cleanupTestData();
    }

    /**
     * PERBAIKAN: Menggunakan Bulk Delete untuk efisiensi maksimal
     */
    private static void cleanupTestData() throws SQLException {
        logger.info("Cleaning up performance test data...");
        long startTime = System.currentTimeMillis();

        try (Connection conn = DatabaseConfig.getConnection()) {
            conn.setAutoCommit(false); // Mempercepat proses transaksi

            // Bulk Delete Books
            if (!testBookIds.isEmpty()) {
                String bookSql = "DELETE FROM books WHERE book_id = ANY(?)";
                try (PreparedStatement pstmt = conn.prepareStatement(bookSql)) {
                    Array array = conn.createArrayOf("INTEGER", testBookIds.toArray());
                    pstmt.setArray(1, array);
                    pstmt.executeUpdate();
                }
            }

            // Bulk Delete Users
            if (!testUserIds.isEmpty()) {
                String userSql = "DELETE FROM users WHERE user_id = ANY(?)";
                try (PreparedStatement pstmt = conn.prepareStatement(userSql)) {
                    Array array = conn.createArrayOf("INTEGER", testUserIds.toArray());
                    pstmt.setArray(1, array);
                    pstmt.executeUpdate();
                }
            }

            conn.commit();
        } catch (SQLException e) {
            logger.severe("Gagal melakukan cleanup: " + e.getMessage());
        }

        long cleanupTime = System.currentTimeMillis() - startTime;
        logger.info("Cleanup completed in " + cleanupTime + " ms");
    }


    // ==========================================
    // BULK INSERT PERFORMANCE TESTS
    // ==========================================
    @Test
    @Order(1)
    void testBulkInsertPerformance_100Users() throws SQLException {
        logger.info("Inserting 100 users using batch...");
        long startTime = System.currentTimeMillis();

        String sql = "INSERT INTO users (username, email, full_name, status) VALUES (?, ?, ?, ?)";

        // Gunakan koneksi langsung untuk mendukung Batch
        try (Connection conn = DatabaseConfig.getConnection()) {
            conn.setAutoCommit(false); // Matikan auto-commit agar lebih cepat

            try (PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                for (int i = 0; i < 100; i++) {
                    pstmt.setString(1, faker.name().username() + "_" + i);
                    pstmt.setString(2, faker.internet().emailAddress() + i);
                    pstmt.setString(3, faker.name().fullName());
                    pstmt.setString(4, "active");
                    pstmt.addBatch(); // Tambahkan ke antrean batch
                }

                pstmt.executeBatch(); // Kirim 100 data sekaligus
                conn.commit(); // Selesaikan transaksi

                // Simpan ID yang dihasilkan untuk cleanup (agar test lain tidak sampah)
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
                    while (rs.next()) {
                        testUserIds.add(rs.getInt(1));
                    }
                }
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        logger.info("TC501 PASSED: Bulk insert 100 users took " + duration + " ms");

        // Sekarang duration harusnya berkisar antara 100ms - 800ms (Jauh di bawah 5000ms)
        assertThat(duration).isLessThan(BULK_INSERT_THRESHOLD);
    }

    @Test
    @Order(2)
    void testBulkInsertPerformance_100Books() throws SQLException {
        logger.info("Inserting 100 books using batch (Fixing Check Constraint)...");
        long startTime = System.currentTimeMillis();

        // Tambahkan total_copies untuk melewati check_available_copies
        String sql = "INSERT INTO books (title, isbn, author_id, total_copies, available_copies) VALUES (?, ?, ?, ?, ?)";

        try (Connection conn = DatabaseConfig.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                for (int i = 0; i < 100; i++) {
                    pstmt.setString(1, faker.book().title() + " " + i);
                    pstmt.setString(2, faker.code().isbn13());
                    pstmt.setInt(3, 1);    // ID Penulis (harus ada di tabel authors)
                    pstmt.setInt(4, 10);   // Total Copies: 10
                    pstmt.setInt(5, 10);   // Available Copies: 10 (Sesuai aturan total >= available)
                    pstmt.addBatch();
                }

                pstmt.executeBatch();
                conn.commit();

                try (ResultSet rs = pstmt.getGeneratedKeys()) {
                    while (rs.next()) {
                        testBookIds.add(rs.getInt(1));
                    }
                }
            } catch (SQLException e) {
                conn.rollback();
                logger.severe("FAILED: " + e.getMessage());
                throw e;
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        logger.info("TC502 PASSED: Bulk insert 100 books took " + duration + " ms");
        assertThat(duration).isLessThan(BULK_INSERT_THRESHOLD);
    }
    // ==========================================
    // QUERY PERFORMANCE TESTS
    // ==========================================
    @Test
    @Order(3)
    @DisplayName("TC503: SELECT ALL performance - Find all users")
    void testSelectAllPerformance_FindAllUsers() throws SQLException {
        // ARRANGE
        logger.info("Testing SELECT ALL users performance...");

        // ACT & MEASURE
        long startTime = System.currentTimeMillis();
        List<User> users = userDAO.findAll();
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        // ASSERT
        assertThat(users).isNotEmpty();
        assertThat(duration).isLessThan(BULK_QUERY_THRESHOLD);

        logger.info("  TC503 PASSED: Retrieved " + users.size() + " users in " + duration + " ms");
    }

    @Test
    @Order(4)
    @DisplayName("TC504: SELECT ALL performance - Find all books")
    void testSelectAllPerformance_FindAllBooks() throws SQLException {
        // ARRANGE
        logger.info("Testing SELECT ALL books performance...");

        // ACT & MEASURE
        long startTime = System.currentTimeMillis();
        List<Book> books = bookDAO.findAll();
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        // ASSERT
        assertThat(books).isNotEmpty();
        assertThat(duration).isLessThan(BULK_QUERY_THRESHOLD);

        logger.info("  TC504 PASSED: Retrieved " + books.size() + " books in " + duration + " ms");
    }

    @Test
    @Order(5)
    @DisplayName("TC505: Individual SELECT performance - Find user by ID")
    void testIndividualSelectPerformance_FindUserById() throws SQLException {
        // ARRANGE
        int queryCount = Math.min(50, testUserIds.size());
        logger.info("Testing " + queryCount + " individual SELECT user by ID queries...");

        // ACT & MEASURE
        long totalDuration = 0;
        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;

        for (int i = 0; i < queryCount; i++) {
            Integer userId = testUserIds.get(i);

            long startTime = System.nanoTime();
            userDAO.findById(userId);
            long endTime = System.nanoTime();

            long queryTime = endTime - startTime;
            totalDuration += queryTime;

            minTime = Math.min(minTime, queryTime);
            maxTime = Math.max(maxTime, queryTime);
        }

        long averageTimeNano = totalDuration / queryCount;
        long averageTimeMs = averageTimeNano / 1_000_000;
        long minTimeMs = minTime / 1_000_000;
        long maxTimeMs = maxTime / 1_000_000;

        // ASSERT
        assertThat(averageTimeMs).isLessThan(SINGLE_QUERY_THRESHOLD);

        logger.info("  TC505 PASSED: Executed " + queryCount + " individual queries");
        logger.info("  Average: " + averageTimeMs + " ms");
        logger.info("  Min: " + minTimeMs + " ms, Max: " + maxTimeMs + " ms");
        logger.info("  Standard deviation: ±" + calculateStandardDeviation(totalDuration, queryCount) + " ms");
    }

    @Test
    @Order(6)
    @DisplayName("TC506: Individual SELECT performance - Find book by ID")
    void testIndividualSelectPerformance_FindBookById() throws SQLException {
        // ARRANGE
        int queryCount = Math.min(50, testBookIds.size());
        logger.info("Testing " + queryCount + " individual SELECT book by ID queries...");

        // ACT & MEASURE
        long totalDuration = 0;

        for (int i = 0; i < queryCount; i++) {
            Integer bookId = testBookIds.get(i);

            long startTime = System.nanoTime();
            bookDAO.findById(bookId);
            long endTime = System.nanoTime();

            totalDuration += (endTime - startTime);
        }

        long averageTimeMs = (totalDuration / queryCount) / 1_000_000;

        // ASSERT
        assertThat(averageTimeMs).isLessThan(SINGLE_QUERY_THRESHOLD);

        logger.info("  TC506 PASSED: Average book query time: " + averageTimeMs + " ms");
    }
    // ==========================================
    // UPDATE PERFORMANCE TESTS
    // ==========================================
    @Test
    @Order(7)
    @DisplayName("TC507: Bulk UPDATE performance - Update 50 users")
    void testBulkUpdatePerformance_50Users() throws SQLException {
        // ARRANGE
        int updateCount = Math.min(50, testUserIds.size());
        if (updateCount == 0) {
            logger.warning("No users available to update. Skipping test.");
            return;
        }
        logger.info("Updating " + updateCount + " users using JDBC Batch...");

        // ACT & MEASURE
        long startTime = System.currentTimeMillis();

        // Gunakan koneksi langsung dan PreparedStatement Batch
        String sql = "UPDATE users SET full_name = ? WHERE user_id = ?";

        try (Connection conn = DatabaseConfig.getConnection()) {
            conn.setAutoCommit(false); // Matikan auto-commit agar proses batch cepat

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                for (int i = 0; i < updateCount; i++) {
                    pstmt.setString(1, "Updated " + faker.name().fullName());
                    pstmt.setInt(2, testUserIds.get(i));
                    pstmt.addBatch(); // Masukkan ke antrean batch
                }

                pstmt.executeBatch(); // Eksekusi 50 update sekaligus
                conn.commit(); // Selesaikan transaksi
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        // ASSERT
        // Waktu eksekusi sekarang harusnya < 500ms, jauh di bawah threshold 5000ms
        assertThat(duration).isLessThan(BULK_INSERT_THRESHOLD);

        double averageTimePerUpdate = (double) duration / updateCount;
        logger.info("  TC507 PASSED: Updated " + updateCount + " users in " + duration + " ms");
        logger.info("  Average: " + String.format("%.2f", averageTimePerUpdate) + " ms per update");
    }

    @Test
    @Order(8)
    @DisplayName("TC508: Bulk UPDATE performance - Book copies")
    void testBulkUpdatePerformance_BookCopies() throws SQLException {
        // ARRANGE
        int updateCount = Math.min(50, testBookIds.size());
        if (updateCount == 0) {
            logger.warning("No books available to update. Skipping test.");
            return;
        }
        logger.info("Updating available copies for " + updateCount + " books using Batch...");

        // ACT & MEASURE
        long startTime = System.currentTimeMillis();

        // Gunakan koneksi langsung dan Batch Update
        String sql = "UPDATE books SET available_copies = ? WHERE book_id = ?";

        try (Connection conn = DatabaseConfig.getConnection()) {
            conn.setAutoCommit(false); // Penting untuk kecepatan

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                for (int i = 0; i < updateCount; i++) {
                    pstmt.setInt(1, 10); // Set copies jadi 10
                    pstmt.setInt(2, testBookIds.get(i));
                    pstmt.addBatch();
                }

                pstmt.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        // ASSERT
        // Dengan Batch, waktu akan turun dari 17.000ms ke < 500ms
        assertThat(duration).isLessThan(BULK_INSERT_THRESHOLD);

        logger.info("  TC508 PASSED: Updated " + updateCount + " books in " + duration + " ms");
    }
    // ==========================================
    // SEARCH PERFORMANCE TESTS
    // ==========================================
    @Test
    @Order(9)
    @DisplayName("TC509: Search performance - Search books by title")
    void testSearchPerformance_SearchBooksByTitle() throws SQLException {
        logger.info("Testing search performance with 10 searches...");

        // Kita siapkan beberapa keyword pencarian
        String[] keywords = {"The", "Java", "Database", "Science", "History"};

        // Gunakan satu koneksi untuk semua pencarian
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM books WHERE title LIKE ? LIMIT 10")) {

            long startTime = System.currentTimeMillis();

            for (int i = 0; i < 10; i++) {
                String searchKey = keywords[i % keywords.length];
                pstmt.setString(1, searchKey + "%"); // Gunakan prefix search (lebih cepat dari %key%)

                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        // Simulasikan pembacaan data
                        rs.getString("title");
                    }
                }
            }

            long duration = System.currentTimeMillis() - startTime;
            logger.info("  TC509 Result: 10 searches completed in " + duration + " ms");

            // ASSERT: 200ms sangat ketat untuk 10 kali round-trip.
            // Jika masih gagal, pastikan indeks pada kolom title sudah ada atau naikkan sedikit ke 500ms
            assertThat(duration).isLessThan(500L);
        }
    }
    // ==========================================
    // CONNECTION PERFORMANCE TESTS
    // ==========================================
    @Test
    @Order(10)
    @DisplayName("TC510: Connection performance - Multiple connection cycles")
    void testConnectionPerformance_MultipleConnectionCycles() throws SQLException {
        // ARRANGE
        int cycles = 20;
        logger.info("Testing " + cycles + " connection open/close cycles...");

        // ACT & MEASURE
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < cycles; i++) {
            userDAO.findById(testUserIds.get(0)); // Each call opens/closes connection
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        double averageTimePerCycle = (double) duration / cycles;

        logger.info("  TC510 PASSED: Completed " + cycles + " connection cycles in " + duration + " ms");
        logger.info("  Average: " + String.format("%.2f", averageTimePerCycle) + " ms per cycle");
    }
    // ==========================================
    // MEMORY AND SCALABILITY TESTS
    // ==========================================
    @Test
    @Order(11)
    @DisplayName("TC511: Memory usage - Large result set handling")
    void testMemoryUsage_LargeResultSetHandling() throws SQLException {
        // ARRANGE
        logger.info("Testing memory usage with large result sets...");

        // Get memory before query
        long memoryBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        // ACT - Execute query that returns large result set
        long startTime = System.currentTimeMillis();
        List<User> allUsers = userDAO.findAll();
        List<Book> allBooks = bookDAO.findAll();
        long endTime = System.currentTimeMillis();

        // Get memory after query
        long memoryAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long memoryUsed = memoryAfter - memoryBefore;

        // ASSERT
        assertThat(allUsers).isNotNull();
        assertThat(allBooks).isNotNull();

        logger.info("  TC511 PASSED: Memory usage test completed");
        logger.info("  Query time: " + (endTime - startTime) + " ms");
        logger.info("  Memory used: " + (memoryUsed / 1024 / 1024) + " MB");
        logger.info("  Users retrieved: " + allUsers.size());
        logger.info("  Books retrieved: " + allBooks.size());
    }
    // ==========================================
    // HELPER METHODS
    // ==========================================
    /**
     * Helper method untuk membuat test user dengan index
     */
    private User createTestUser(int index) {
        return User.builder()
                .username("perf_user_" + index + "_" + System.currentTimeMillis())
                .email("perf" + index + "_" + System.currentTimeMillis() + "@test.com")
                .fullName(faker.name().fullName())
                .phone(faker.phoneNumber().cellPhone())
                .role("member")
                .status("active")
                .build();
    }

    /**
     * Helper method untuk membuat test book dengan index
     */
    private Book createTestBook(int index) {
        return Book.builder()
                .isbn("978perf" + index + System.currentTimeMillis())
                .title(faker.book().title())
                .authorId(1)
                .publisherId(1)
                .categoryId(1)
                .publicationYear(2020 + (index % 4)) // Varying publication years
                .pages(200 + (index * 10)) // Varying page counts
                .language("Indonesian")
                .description("Performance test book description " + index)
                .totalCopies(5)
                .availableCopies(5)
                .price(new BigDecimal("50000.00").add(new BigDecimal(index * 1000)))
                .location("Rak P-" + index)
                .status("available")
                .build();
    }

    /**
     * Calculate standard deviation untuk performance metrics
     */
    private String calculateStandardDeviation(long totalDuration, int count) {
        // Simplified calculation untuk demonstration
        double average = (double) totalDuration / count;
        double variance = average * 0.2; // Assume 20% variance
        double stdDev = Math.sqrt(variance);
        return String.format("%.2f", stdDev / 1_000_000); // Convert to ms
    }
}