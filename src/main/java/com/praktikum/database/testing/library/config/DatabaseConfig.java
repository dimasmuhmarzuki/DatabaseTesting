package com.praktikum.database.testing.library.config;
// Import classes untuk handle I/O dan database operations
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Singleton class untuk mengelola koneksi database
 * Handle loading configuration, establishing connection, dan error handling
 */
public class DatabaseConfig {
    // Logger untuk mencatat aktivitas dan error
    private static final Logger logger = Logger.getLogger(DatabaseConfig.class.getName());

    // Properties object untuk menyimpan configuration dari file
    private static Properties properties = new Properties();

    // Database connection parameters
    private static String DB_URL;
    private static String DB_USERNAME;
    private static String DB_PASSWORD;
    private static String DB_DRIVER;

    // Static initialization block - dieksekusi sekali ketika class pertama kali loaded
    static {
        // Load configuration ketika class pertama kali di-load
        loadProperties();
        // Test koneksi database saat startup
        testConnection();
    }

    /**
     * Load database configuration dari file database.properties
     * Method ini dipanggil otomatis ketika class pertama kali di-load
     */
    private static void loadProperties() {
        // Try-with-resources untuk auto-close InputStream
        try (InputStream input = DatabaseConfig.class
                .getClassLoader()
                .getResourceAsStream("database.properties")) {

            // Check jika file properties tidak ditemukan
            if (input == null) {
                throw new RuntimeException("Error: File database.properties tidak ditemukan di classpath");
            }

            // Load properties dari file
            properties.load(input);

            // Baca nilai dari properties file
            DB_URL = properties.getProperty("db.url");
            DB_USERNAME = properties.getProperty("db.username");
            DB_PASSWORD = properties.getProperty("db.password");
            DB_DRIVER = properties.getProperty("db.driver");

            // Validasi bahwa semua configuration required ada
            validateConfiguration();

            // Load JDBC driver class
            Class.forName(DB_DRIVER);

            logger.info("Database configuration berhasil di-load");

        } catch (IOException | ClassNotFoundException e) {
            // Log error dan throw runtime exception
            logger.severe("Error: Gagal load database configuration: " + e.getMessage());
            throw new RuntimeException("Error konfigurasi database", e);
        }
    }

    /**
     * Validasi bahwa semua configuration required sudah di-set
     * Memastikan tidak ada property yang null atau empty
     */
    private static void validateConfiguration() {
        if (DB_URL == null || DB_URL.trim().isEmpty()) {
            throw new RuntimeException("Database URL harus diisi");
        }
        if (DB_USERNAME == null || DB_USERNAME.trim().isEmpty()) {
            throw new RuntimeException("Database username harus diisi");
        }
        if (DB_PASSWORD == null || DB_PASSWORD.trim().isEmpty()) {
            throw new RuntimeException("Database password harus diisi");
        }
        if (DB_DRIVER == null || DB_DRIVER.trim().isEmpty()) {
            throw new RuntimeException("Database driver harus diisi");
        }
    }

    /**
     * Mendapatkan koneksi database baru
     * @return Connection object untuk berinteraksi dengan database
     * @throws SQLException jika gagal membuat koneksi
     */
    public static Connection getConnection() throws SQLException {
        // Gunakan DriverManager untuk membuat koneksi
        return DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
    }

    /**
     * Menutup koneksi database
     * @param conn Koneksi yang akan ditutup
     */
    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
                logger.fine("Koneksi database ditutup");
            } catch (SQLException e) {
                logger.warning("Gagal menutup koneksi database: " + e.getMessage());
            }
        }
    }

    /**
     * Test koneksi ke database
     * @return true jika koneksi berhasil, false jika gagal
     */
    public static boolean testConnection() {
        // Try-with-resources untuk auto-close connection
        try (Connection conn = getConnection()) {
            // Check jika koneksi valid dan tidak closed
            boolean isValid = conn != null && !conn.isClosed();
            if (isValid) {
                logger.info("Test koneksi database: BERHASIL");
            } else {
                logger.severe("Test koneksi database: GAGAL - Koneksi null atau closed");
            }
            return isValid;
        } catch (SQLException e) {
            logger.severe("Test koneksi database: GAGAL - " + e.getMessage());
            return false;
        }
    }

    /**
     * Print informasi database untuk debugging purposes
     */
    public static void printDatabaseInfo() {
        try (Connection conn = getConnection()) {
            // Dapatkan metadata database
            var metaData = conn.getMetaData();
            logger.info("Informasi Database:");
            logger.info("Product: " + metaData.getDatabaseProductName());
            logger.info("Version: " + metaData.getDatabaseProductVersion());
            logger.info("URL: " + metaData.getURL());
            logger.info("User: " + metaData.getUserName());
            logger.info("Driver: " + metaData.getDriverName() + " " + metaData.getDriverVersion());
        } catch (SQLException e) {
            logger.warning("Gagal mendapatkan info database: " + e.getMessage());
        }
    }

    /**
     * Getter untuk database URL (digunakan untuk testing)
     */
    public static String getDbUrl() {
        return DB_URL;
    }

    /**
     * Getter untuk database username (digunakan untuk testing)
     */
    public static String getDbUsername() {
        return DB_USERNAME;
    }
}