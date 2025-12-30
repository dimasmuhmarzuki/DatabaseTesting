package com.praktikum.database.testing.library;
// Import classes untuk testing dan database
import com.praktikum.database.testing.library.config.DatabaseConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

/**
 * Base class untuk semua database tests
 * Menyediakan common setup, cleanup, dan helper methods
 * Mengimplementasikan test isolation menggunakan transaction rollback
 */
public class BaseDatabaseTest {
    // Logger untuk mencatat aktivitas testing
    protected static final Logger logger =
            Logger.getLogger(BaseDatabaseTest.class.getName());

    // Database connection yang digunakan untuk tests
    protected Connection connection;

    /**
     * Setup method yang dijalankan sekali sebelum semua tests
     * Memvalidasi koneksi database tersedia
     */
    @BeforeAll
    static void setUpAll() {
        logger.info("================================================");
        logger.info(" Setting up Database Test Environment");
        logger.info("================================================");

        // Test koneksi database
        boolean connected = DatabaseConfig.testConnection();
        if (!connected) {
            throw new RuntimeException("Tidak bisa terkoneksi ke database. Tests tidak bisa dijalankan.");
        }

        // Print database info untuk debugging
        DatabaseConfig.printDatabaseInfo();
    }

    /**
     * Setup method yang dijalankan sebelum setiap test method
     * Membuat koneksi database baru dan set auto-commit false untuk isolation
     */
    @BeforeEach
    void setUp() throws SQLException {
        // Dapatkan koneksi database baru
        connection = DatabaseConfig.getConnection();

        // Set auto-commit false untuk implementasi test isolation
        // Semua changes akan di-rollback setelah test
        connection.setAutoCommit(false);

        logger.info("Test connection established - Auto-commit: false");
    }

    /**
     * Cleanup method yang dijalankan setelah setiap test method
     * Rollback semua changes dan close connection
     */
    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            try {
                // Rollback semua changes yang dibuat selama test
                connection.rollback();
                logger.info("All test changes rolled back");
            } catch (SQLException e) {
                logger.warning("Warning: Gagal rollback: " + e.getMessage());
            } finally {
                // Tutup koneksi
                connection.close();
                logger.info("Test connection closed");
            }
        }
    }

    /**
     * Helper method untuk membersihkan data di table tertentu
     * Berguna untuk tests yang perlu clean state
     * @param tableName Nama table yang akan dibersihkan
     */
    protected void cleanUpTable(String tableName) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // Hati-hati! Ini akan menghapus semua data di table
            int deletedRows = stmt.executeUpdate("DELETE FROM " + tableName);
            logger.info("Cleaned up table '" + tableName + "' - Deleted " + deletedRows + " rows");
        } catch (SQLException e) {
            logger.warning("Gagal clean up table '" + tableName + "': " + e.getMessage());
            throw e;
        }
    }

    /**
     * Helper method untuk menghitung jumlah records di table
     * @param tableName Nama table yang akan dihitung
     * @return Jumlah records di table
     */
    protected int countRecords(String tableName) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            var rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName);
            int count = rs.next() ? rs.getInt(1) : 0;
            logger.fine("Record count in '" + tableName + "': " + count);
            return count;
        }
    }

    /**
     * Helper method untuk execute SQL query
     * @param sql SQL query yang akan di-execute
     */
    protected void executeSQL(String sql) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            logger.fine("Executed SQL: " + sql);
        }
    }

    /**
     * Helper method untuk pause execution (useful untuk testing timestamps)
     * @param milliseconds Jumlah milliseconds untuk pause
     */
    protected void pause(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
            logger.fine("Paused for " + milliseconds + "ms");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warning("Sleep interrupted: " + e.getMessage());
        }
    }
}