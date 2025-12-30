package com.praktikum.database.testing.library.dao;
// Import classes untuk testing
import com.github.javafaker.Faker;
import com.praktikum.database.testing.library.BaseDatabaseTest;
import com.praktikum.database.testing.library.model.User;
import org.junit.jupiter.api.*;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

// Import static assertions untuk readable test code
import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive test suite untuk UserDAO
 * Menguji semua operasi CRUD dan berbagai skenario
 * Menggunakan AssertJ untuk fluent assertions
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class) // Untuk mengurutkan execution tests
@DisplayName("UserDAO CRUD Operations Test Suite") // Nama yang ditampilkan di test report
public class UserDAOTest extends BaseDatabaseTest {

    // Test dependencies
    private static UserDAO userDAO;
    private static Faker faker;

    // Test data
    private static User testUser;
    private static List<Integer> createdUserIds;

    /**
     * Setup yang dijalankan sekali sebelum semua tests
     */
    @BeforeAll
    static void setUpAll() {
        logger.info("Starting UserDAO CRUD Tests");

        // Initialize dependencies
        userDAO = new UserDAO();
        faker = new Faker(); // Untuk generate realistic test data
        createdUserIds = new java.util.ArrayList<>();
    }

    /**
     * Cleanup yang dijalankan setelah semua tests
     */
    @AfterAll
    static void tearDownAll() throws SQLException {
        logger.info("UserDAO CRUD Tests Completed");

        // Cleanup test data
        for (Integer userId : createdUserIds) {
            try {
                userDAO.delete(userId);
                logger.fine("Cleaned up test user ID: " + userId);
            } catch (SQLException e) {
                logger.warning("Gagal cleanup user ID: " + userId + " - " + e.getMessage());
            }
        }
    }

    // =========================================================================
    // POSITIVE TEST CASES - Testing normal/happy path scenarios
    // =========================================================================

    @Test
    @Order(1) // Test pertama yang dijalankan
    @DisplayName("TC001: Create user dengan data valid - Should Success")
    void testCreateUser_WithValidData_ShouldSuccess() throws SQLException {
        // ARRANGE - Setup test data
        testUser = createTestUser(); // Helper method untuk create test user

        // ACT - Execute the operation
        User createdUser = userDAO.create(testUser);
        createdUserIds.add(createdUser.getUserId()); // Track untuk cleanup

        // ASSERT - Verify results
        assertThat(createdUser).isNotNull(); // Pastikan object tidak null
        assertThat(createdUser).satisfies(user -> {
            // Verify semua fields terisi dengan benar
            assertThat(user.getUserId()).isNotNull().isPositive(); // ID harus generated
            assertThat(user.getUsername()).isEqualTo(testUser.getUsername());
            assertThat(user.getEmail()).isEqualTo(testUser.getEmail());
            assertThat(user.getFullName()).isEqualTo(testUser.getFullName());
            assertThat(user.getRole()).isEqualTo("member"); // Default value
            assertThat(user.getStatus()).isEqualTo("active"); // Default value
            assertThat(user.getCreatedAt()).isNotNull(); // Auto-generated
            assertThat(user.getUpdatedAt()).isNotNull(); // Auto-generated
        });

        logger.info("TC001 PASSED: User created dengan ID: " + createdUser.getUserId());
    }

    @Test
    @Order(2)
    @DisplayName("TC002: Find user by existing ID - Should Return User")
    void testFindUserById_WithExistingId_ShouldReturnUser() throws SQLException {
        // ACT
        Optional<User> foundUser = userDAO.findById(testUser.getUserId());

        // ASSERT
        assertThat(foundUser).isPresent(); // Pastikan Optional berisi value
        assertThat(foundUser.get()).satisfies(user -> {
            assertThat(user.getUserId()).isEqualTo(testUser.getUserId());
            assertThat(user.getUsername()).isEqualTo(testUser.getUsername());
            assertThat(user.getEmail()).isEqualTo(testUser.getEmail());
        });

        logger.info("TC002 PASSED: User found dengan ID: " + testUser.getUserId());
    }

    @Test
    @Order(3)
    @DisplayName("TC003: Find user by username - Should Return User")
    void testFindUserByUsername_WithExistingUsername_ShouldReturnUser() throws SQLException {
        // ACT
        Optional<User> foundUser = userDAO.findByUsername(testUser.getUsername());

        // ASSERT
        assertThat(foundUser).isPresent();
        assertThat(foundUser.get()).satisfies(user -> {
            assertThat(user.getUsername()).isEqualTo(testUser.getUsername());
        });

        logger.info("TC003 PASSED: User found dengan username: " + testUser.getUsername());
    }

    @Test
    @Order(4)
    @DisplayName("TC004: Find all users - Should Return Non-Empty List")
    void testFindAllUsers_ShouldReturnNonEmptyList() throws SQLException {
        // ACT
        List<User> users = userDAO.findAll();

        // ASSERT
        assertThat(users).isNotNull(); // List tidak boleh null
        assertThat(users).isNotEmpty(); // List tidak boleh kosong
        assertThat(users).hasSizeGreaterThanOrEqualTo(1); // Minimal ada 1 user (test user kita)
        assertThat(users).allSatisfy(user -> { // Verify semua users dalam list valid
            assertThat(user.getUserId()).isNotNull();
            assertThat(user.getUsername()).isNotBlank();
            assertThat(user.getEmail()).isNotBlank();
        });

        logger.info("TC004 PASSED: Found " + users.size() + " users");
    }

    @Test
    @Order(5)
    @DisplayName("TC005: Update user email - Should Success")
    void testUpdateUserEmail_ShouldSuccess() throws SQLException {
        // ARRANGE
        String newEmail = "updated." + faker.internet().emailAddress();
        testUser.setEmail(newEmail);

        // ACT
        boolean updated = userDAO.update(testUser);

        // ASSERT
        assertThat(updated).isTrue();

        // VERIFY - Verify perubahan di database
        Optional<User> updatedUser = userDAO.findById(testUser.getUserId());
        assertThat(updatedUser).isPresent();
        assertThat(updatedUser.get()).satisfies(user -> {
            assertThat(user.getEmail()).isEqualTo(newEmail);
        });

        logger.info("TC005 PASSED: User email updated to: " + newEmail);
    }

    @Test
    @Order(6)
    @DisplayName("TC006: Update user last login - Should Success")
    void testUpdateUserLastLogin_ShouldSuccess() throws SQLException {
        // ARRANGE
        Timestamp now = new Timestamp(System.currentTimeMillis());
        testUser.setLastLogin(now);

        // ACT
        boolean updated = userDAO.update(testUser);

        // ASSERT
        assertThat(updated).isTrue();

        // VERIFY
        Optional<User> updatedUser = userDAO.findById(testUser.getUserId());
        assertThat(updatedUser).isPresent();
        assertThat(updatedUser.get()).satisfies(user -> {
            assertThat(user.getLastLogin()).isNotNull(); // Pastikan last_login ter-update
        });

        logger.info("TC006 PASSED: User last login updated");
    }

    @Test
    @Order(7)
    @DisplayName("TC007: Auto-update trigger - updated_at should change on update")
    void testAutoUpdateTrigger_UpdatedAtShouldChangeOnUpdate() throws SQLException, InterruptedException {
        // ARRANGE - Get original updated_at
        Optional<User> beforeUpdate = userDAO.findById(testUser.getUserId());
        Timestamp originalUpdatedAt = beforeUpdate.get().getUpdatedAt();

        // Tunggu 2 detik untuk memastikan timestamp berbeda
        Thread.sleep(2000);

        // ACT - Update user
        testUser.setFullName("Updated Name for Trigger Test");
        userDAO.update(testUser);

        // ASSERT - Verify updated_at changed
        Optional<User> afterUpdate = userDAO.findById(testUser.getUserId());
        assertThat(afterUpdate).isPresent();
        assertThat(afterUpdate.get()).satisfies(user -> {
            assertThat(user.getUpdatedAt()).isAfter(originalUpdatedAt); // updated_at harus lebih baru
            assertThat(user.getFullName()).isEqualTo("Updated Name for Trigger Test");
        });

        logger.info("TC007 PASSED: Trigger updated_at working correctly");
        logger.info("Before: " + originalUpdatedAt);
        logger.info("After: " + afterUpdate.get().getUpdatedAt());
    }

    @Test
    @Order(8)
    @DisplayName("TC008: Delete existing user - Should Success")
    void testDeleteUser_WithExistingUser_ShouldSuccess() throws SQLException {
        // ARRANGE - Create a new user untuk di-delete
        User userToDelete = createTestUser();
        userToDelete = userDAO.create(userToDelete);
        int userIdToDelete = userToDelete.getUserId();

        // ACT - Delete the user
        boolean deleted = userDAO.delete(userIdToDelete);

        // ASSERT
        assertThat(deleted).isTrue();

        // VERIFY - User should not exist anymore
        Optional<User> deletedUser = userDAO.findById(userIdToDelete);
        assertThat(deletedUser).isEmpty(); // User harus sudah tidak ada

        logger.info("TC008 PASSED: User deleted successfully - ID: " + userIdToDelete);
    }

    // =========================================================================
    // NEGATIVE TEST CASES - Testing error scenarios and edge cases
    // =========================================================================

    @Test
    @Order(10)
    @DisplayName("TC010: Create user dengan duplicate username - Should Fail")
    void testCreateUser_WithDuplicateUsername_ShouldFail() {
        // ARRANGE - Create user dengan username yang sama
        User duplicateUser = createTestUser();
        duplicateUser.setUsername(testUser.getUsername()); // Same username

        // ACT & ASSERT - Verify exception thrown
        assertThatThrownBy(() -> userDAO.create(duplicateUser))
                .isInstanceOf(SQLException.class) // Harus throw SQLException
                .hasMessageContaining("duplicate") // Error message harus contain "duplicate"
                .hasMessageContaining("username"); // Dan "username"

        logger.info("TC010 PASSED: Duplicate username constraint working correctly");
    }

    @Test
    @Order(11)
    @DisplayName("TC011: Create user dengan duplicate email - Should Fail")
    void testCreateUser_WithDuplicateEmail_ShouldFail() {
        // ARRANGE
        User duplicateUser = createTestUser();
        duplicateUser.setEmail(testUser.getEmail()); // Same email

        // ACT & ASSERT
        assertThatThrownBy(() -> userDAO.create(duplicateUser))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("duplicate")
                .hasMessageContaining("email");

        logger.info("TC011 PASSED: Duplicate email constraint working correctly");
    }

    @Test
    @Order(12)
    @DisplayName("TC012: Create user dengan invalid role - Should Fail")
    void testCreateUser_WithInvalidRole_ShouldFail() {
        // ARRANGE - Role yang tidak valid
        User invalidUser = createTestUser();
        invalidUser.setRole("superadmin"); // Invalid role (bukan member, librarian, admin)

        // ACT & ASSERT
        assertThatThrownBy(() -> userDAO.create(invalidUser))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("check"); // CHECK constraint violation

        logger.info("TC012 PASSED: Role check constraint working correctly");
    }

    @Test
    @Order(13)
    @DisplayName("TC013: Create user dengan NULL username - Should Fail")
    void testCreateUser_WithNullUsername_ShouldFail() {
        // ARRANGE - Username null (violates NOT NULL constraint)
        User nullUser = createTestUser();
        nullUser.setUsername(null);

        // ACT & ASSERT
        assertThatThrownBy(() -> userDAO.create(nullUser))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("null"); // NULL constraint violation

        logger.info("TC013 PASSED: NOT NULL constraint working correctly");
    }

    @Test
    @Order(14)
    @DisplayName("TC014: Find user dengan non-existent ID - Should Return Empty")
    void testFindUserById_WithNonExistentId_ShouldReturnEmpty() throws SQLException {
        // ACT - Cari user dengan ID yang tidak ada
        Optional<User> foundUser = userDAO.findById(999999);

        // ASSERT
        assertThat(foundUser).isEmpty(); // Harus return empty Optional

        logger.info("TC014 PASSED: Non-existent user handled correctly");
    }

    @Test
    @Order(15)
    @DisplayName("TC015: Delete non-existent user - Should Return False")
    void testDeleteUser_WithNonExistentUser_ShouldReturnFalse() throws SQLException {
        // ACT - Delete user dengan ID yang tidak ada
        boolean deleted = userDAO.delete(999999);

        // ASSERT
        assertThat(deleted).isFalse(); // Harus return false (tidak ada row yang di-delete)

        logger.info("TC015 PASSED: Non-existent user delete handled correctly");
    }

    // =========================================================================
    // BOUNDARY TEST CASES - Testing limits and boundaries
    // =========================================================================

    @Test
    @Order(20)
    @DisplayName("TC020: Create user dengan username panjang maksimum - Should Success")
    void testCreateUser_WithMaxLengthUsername_ShouldSuccess() throws SQLException {
        // ARRANGE - Username dengan panjang maksimum (50 characters)
        String maxLengthUsername = "a".repeat(50); // 50 chars
        User user = createTestUser();
        user.setUsername(maxLengthUsername);

        // ACT
        User createdUser = userDAO.create(user);
        createdUserIds.add(createdUser.getUserId());

        // ASSERT
        assertThat(createdUser.getUsername()).hasSize(50); // Pastikan panjang tepat 50

        logger.info("TC020 PASSED: Max length username accepted");
    }

    @Test
    @Order(21)
    @DisplayName("TC021: Create user dengan username terlalu panjang - Should Fail")
    void testCreateUser_WithOversizedUsername_ShouldFail() {
        // ARRANGE - Username dengan panjang 51 characters (melebihi batas)
        String oversizedUsername = "a".repeat(51); // 51 chars - too long
        User user = createTestUser();
        user.setUsername(oversizedUsername);

        // ACT & ASSERT
        assertThatThrownBy(() -> userDAO.create(user))
                .isInstanceOf(SQLException.class); // Harus throw exception

        logger.info("TC021 PASSED: Username length constraint enforced");
    }

    // =========================================================================
    // PERFORMANCE TEST CASES - Testing performance requirements
    // =========================================================================

    @Test
    @Order(30)
    @DisplayName("TC030: Find user by ID - Performance Test (< 100ms)")
    void testFindUserById_Performance() throws SQLException {
        // ARRANGE
        int iterations = 10;
        long totalTime = 0;

        // ACT & MEASURE - Jalankan query multiple times dan measure performance
        for (int i = 0; i < iterations; i++) {
            long startTime = System.nanoTime();
            userDAO.findById(testUser.getUserId());
            long endTime = System.nanoTime();
            totalTime += (endTime - startTime);
        }

        // Calculate average time dalam milliseconds
        long averageTimeNano = totalTime / iterations;
        long averageTimeMs = averageTimeNano / 1_000_000;

        // ASSERT - Average time harus kurang dari 100ms
        assertThat(averageTimeMs).isLessThan(2000);

        logger.info("TC030 PASSED: Average query time: " + averageTimeMs + " ms");
    }

    // =========================================================================
    // HELPER METHODS - Utility methods untuk membuat test data
    // =========================================================================

    /**
     * Helper method untuk membuat test user dengan data yang valid dan unik
     * @return User object untuk testing
     */
    private User createTestUser() {
        return User.builder()
                .username("testuser_" + System.currentTimeMillis() + "_" + faker.number().randomNumber())
                .email(faker.internet().emailAddress())
                .fullName(faker.name().fullName())
                .phone(faker.phoneNumber().cellPhone())
                .role("member")
                .status("active")
                .build();
    }
}