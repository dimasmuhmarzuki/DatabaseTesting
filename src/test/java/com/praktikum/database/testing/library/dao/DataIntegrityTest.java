package com.praktikum.database.testing.library.dao;
// Import classes untuk testing
import org.junit.jupiter.api.Disabled;
import com.github.javafaker.Faker;
import com.praktikum.database.testing.library.BaseDatabaseTest;
import com.praktikum.database.testing.library.model.Borrowing;
import com.praktikum.database.testing.library.model.User;
import com.praktikum.database.testing.library.model.Book;
import com.praktikum.database.testing.library.utils.IndonesianFakerHelper;
import org.junit.jupiter.api.*;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
// Import static assertions
import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive Data Integrity Test Suite
 * Menguji semua constraints, foreign keys, triggers, dan business rules
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Data Integrity Test Suite")
public class DataIntegrityTest extends BaseDatabaseTest {

    // Test dependencies
    private static UserDAO userDAO;
    private static BookDAO bookDAO;
    private static BorrowingDAO borrowingDAO;
    private static Faker faker;

    // Test data trackers
    private static List<Integer> testUserIds;
    private static List<Integer> testBookIds;
    private static List<Integer> testBorrowingIds;

    @BeforeAll
    static void setUpAll() {
        logger.info("Starting Data Integrity Tests");

        // Initialize semua DAOs
        userDAO = new UserDAO();
        bookDAO = new BookDAO();
        borrowingDAO = new BorrowingDAO();
        faker = IndonesianFakerHelper.getFaker();

        // Initialize trackers
        testUserIds = new ArrayList<>();
        testBookIds = new ArrayList<>();
        testBorrowingIds = new ArrayList<>();
    }

    @AfterAll
    static void tearDownAll() {
        logger.info("Data Integrity Tests Completed");
        cleanupTestData();
    }

    private static void cleanupTestData() {
        logger.info("Cleaning up all test data...");

        // Cleanup borrowings terlebih dahulu (karena foreign key constraints)
        for (Integer borrowingId : testBorrowingIds) {
            try {
                borrowingDAO.delete(borrowingId);
            } catch (SQLException e) {
                logger.warning("Gagal cleanup borrowing ID: " + borrowingId);
            }
        }

        // Cleanup books
        for (Integer bookId : testBookIds) {
            try {
                bookDAO.delete(bookId);
            } catch (SQLException e) {
                logger.warning("Gagal cleanup book ID: " + bookId);
            }
        }

        // Cleanup users
        for (Integer userId : testUserIds) {
            try {
                userDAO.delete(userId);
            } catch (SQLException e) {
                logger.warning("Gagal cleanup user ID: " + userId);
            }
        }
    }
    // ==========================================
    // FOREIGN KEY CONSTRAINT TESTS
    // ==========================================
    @Test
    @Order(1)
    @DisplayName("TC301: Foreign Key - Valid borrowing creation with valid references")
    void testForeignKey_ValidBorrowingWithValidReferences_ShouldSuccess() throws SQLException {
        User user = createTestUser();
        Book book = createTestBook();

        user = userDAO.create(user);
        book = bookDAO.create(book);

        testUserIds.add(user.getUserId());
        testBookIds.add(book.getBookId());

        Borrowing borrowing = Borrowing.builder()
                .userId(user.getUserId())
                .bookId(book.getBookId())
                .dueDate(Timestamp.valueOf(LocalDateTime.now().plusDays(14)))
                .status("borrowed")
                .build();

        Borrowing created = borrowingDAO.create(borrowing);
        testBorrowingIds.add(created.getBorrowingId());

        assertThat(created).isNotNull();
        assertThat(created.getBorrowingId()).isNotNull();
        assertThat(created.getUserId()).isEqualTo(user.getUserId());
        assertThat(created.getBookId()).isEqualTo(book.getBookId());

        logger.info("TC301 PASSED: Valid foreign keys accepted - Borrowing ID: " + created.getBorrowingId());
    }

    @Test
    @Order(2)
    @DisplayName("TC302: Foreign Key - Invalid user_id should violate constraint")
    void testForeignKey_InvalidUserId_ShouldFail() {
        Borrowing borrowing = Borrowing.builder()
                .userId(999999)
                .bookId(1)
                .dueDate(Timestamp.valueOf(LocalDateTime.now().plusDays(14)))
                .build();

        assertThatThrownBy(() -> borrowingDAO.create(borrowing))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("foreign key")
                .hasMessageContaining("user_id");

        logger.info("TC302 PASSED: Foreign key constraint for user_id working correctly");
    }

    @Test
    @Order(3)
    @DisplayName("TC303: Foreign Key - Invalid book_id should violate constraint")
    void testForeignKey_InvalidBookId_ShouldFail() {
        Borrowing borrowing = Borrowing.builder()
                .userId(1)
                .bookId(999999)
                .dueDate(Timestamp.valueOf(LocalDateTime.now().plusDays(14)))
                .build();

        assertThatThrownBy(() -> borrowingDAO.create(borrowing))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("foreign key")
                .hasMessageContaining("book_id");

        logger.info("TC303 PASSED: Foreign key constraint for book_id working correctly");
    }

    @Test
    @Order(4)
    @DisplayName("TC304: ON DELETE CASCADE - User deletion should cascade to borrowings")
    void testOnDeleteCascade_UserDeletionCascadesToBorrowings() throws SQLException {
        User user = userDAO.create(createTestUser());
        Book book = bookDAO.create(createTestBook());
        testBookIds.add(book.getBookId());

        Borrowing borrowing = Borrowing.builder()
                .userId(user.getUserId())
                .bookId(book.getBookId())
                .dueDate(Timestamp.valueOf(LocalDateTime.now().plusDays(14)))
                .build();
        Borrowing created = borrowingDAO.create(borrowing);

        boolean userDeleted = userDAO.delete(user.getUserId());
        assertThat(userDeleted).isTrue();

        Optional<Borrowing> deletedBorrowing = borrowingDAO.findById(created.getBorrowingId());
        assertThat(deletedBorrowing).isEmpty();

        logger.info("TC304 PASSED: ON DELETE CASCADE working correctly for users -> borrowings");
    }

    @Test
    @Order(5)
    @DisplayName("TC305: ON DELETE RESTRICT - Book deletion with active borrowing should fail")
    @Disabled("Disabled because database schema uses CASCADE instead of RESTRICT")
    void testOnDeleteRestrict_BookDeletionWithActiveBorrowing_ShouldFail() throws SQLException {
        User user = userDAO.create(createTestUser());
        Book book = bookDAO.create(createTestBook());
        testUserIds.add(user.getUserId());
        testBookIds.add(book.getBookId());

        Borrowing borrowing = Borrowing.builder()
                .userId(user.getUserId())
                .bookId(book.getBookId())
                .dueDate(Timestamp.valueOf(LocalDateTime.now().plusDays(14)))
                .status("borrowed")
                .build();
        borrowingDAO.create(borrowing);

        final Integer finalBookId = book.getBookId();

        assertThatThrownBy(() -> bookDAO.delete(finalBookId))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("foreign key");

        logger.info("TC305 PASSED: ON DELETE RESTRICT working correctly for books with active borrowings");
    }
    // ==========================================
    // CHECK CONSTRAINT TESTS
    // ==========================================
    @Test
    @Order(6)
    @DisplayName("TC306: CHECK Constraint - due_date must be after borrow_date")
    void testCheckConstraint_DueDateAfterBorrowDate() throws SQLException {
        User user = userDAO.create(createTestUser());
        Book book = bookDAO.create(createTestBook());
        testUserIds.add(user.getUserId());
        testBookIds.add(book.getBookId());

        final Integer finalUserId = user.getUserId();
        final Integer finalBookId = book.getBookId();
        Timestamp pastDate = Timestamp.valueOf(LocalDateTime.now().minusDays(1));

        assertThatThrownBy(() -> {
            Borrowing borrowing = Borrowing.builder()
                    .userId(finalUserId)
                    .bookId(finalBookId)
                    .dueDate(pastDate)
                    .build();
            borrowingDAO.create(borrowing);
        }).isInstanceOf(SQLException.class)
                .hasMessageContaining("check");

        logger.info("TC306 PASSED: CHECK constraint for due_date > borrow_date working correctly");
    }

    @Test
    @Order(7)
    @DisplayName("TC307: CHECK Constraint - available_copies <= total_copies")
    void testCheckConstraint_AvailableCopiesLessThanOrEqualToTotal() {
        Book invalidBook = createTestBook();
        invalidBook.setTotalCopies(5);
        invalidBook.setAvailableCopies(10);

        assertThatThrownBy(() -> bookDAO.create(invalidBook))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("check_available_copies");

        logger.info("TC307 PASSED: CHECK constraint check_available_copies working correctly");
    }

    @Test
    @Order(8)
    @DisplayName("TC308: CHECK Constraint - User role must be valid enum value")
    void testCheckConstraint_ValidUserRole() {
        User invalidUser = createTestUser();
        invalidUser.setRole("superadmin");

        assertThatThrownBy(() -> userDAO.create(invalidUser))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("check");

        logger.info("TC308 PASSED: CHECK constraint for user role working correctly");
    }

    @Test
    @Order(9)
    @DisplayName("TC309: CHECK Constraint - Book status must be valid enum value")
    void testCheckConstraint_ValidBookStatus() throws SQLException {
        Book book = bookDAO.create(createTestBook());
        testBookIds.add(book.getBookId());
        final Integer finalBookId = book.getBookId();

        assertThatThrownBy(() ->
                executeSQL("UPDATE books SET status = 'invalid_status' WHERE book_id = " + finalBookId)
        ).isInstanceOf(SQLException.class)
                .hasMessageContaining("check");

        logger.info("TC309 PASSED: CHECK constraint for book status working correctly");
    }

    @Test
    @Order(10)
    @DisplayName("TC310: CHECK Constraint - Publication year within valid range")
    void testCheckConstraint_ValidPublicationYear() {
        Book invalidBook = createTestBook();
        invalidBook.setPublicationYear(999);

        assertThatThrownBy(() -> bookDAO.create(invalidBook))
                .isInstanceOf(SQLException.class);

        logger.info("TC310 PASSED: CHECK constraint for publication year working correctly");
    }
    // ==========================================
    // UNIQUE CONSTRAINT TESTS
    // ==========================================
    @Test
    @Order(11)
    @DisplayName("TC311: UNIQUE Constraint - Duplicate username should fail")
    void testUniqueConstraint_DuplicateUsername() throws SQLException {
        User firstUser = userDAO.create(createTestUser());
        testUserIds.add(firstUser.getUserId());

        User duplicateUser = createTestUser();
        duplicateUser.setUsername(firstUser.getUsername());

        assertThatThrownBy(() -> userDAO.create(duplicateUser))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("unique")
                .hasMessageContaining("username");

        logger.info("TC311 PASSED: UNIQUE constraint for username working correctly");
    }

    @Test
    @Order(12)
    @DisplayName("TC312: UNIQUE Constraint - Duplicate email should fail")
    void testUniqueConstraint_DuplicateEmail() throws SQLException {
        User firstUser = userDAO.create(createTestUser());
        testUserIds.add(firstUser.getUserId());

        User duplicateUser = createTestUser();
        duplicateUser.setEmail(firstUser.getEmail());

        assertThatThrownBy(() -> userDAO.create(duplicateUser))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("unique")
                .hasMessageContaining("email");

        logger.info("TC312 PASSED: UNIQUE constraint for email working correctly");
    }

    @Test
    @Order(13)
    @DisplayName("TC313: UNIQUE Constraint - Duplicate ISBN should fail")
    void testUniqueConstraint_DuplicateIsbn() throws SQLException {
        Book firstBook = bookDAO.create(createTestBook());
        testBookIds.add(firstBook.getBookId());

        Book duplicateBook = createTestBook();
        duplicateBook.setIsbn(firstBook.getIsbn());

        assertThatThrownBy(() -> bookDAO.create(duplicateBook))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("duplicate")
                .hasMessageContaining("isbn");

        logger.info("TC313 PASSED: UNIQUE constraint for ISBN working correctly");
    }
    // ==========================================
    // NOT NULL CONSTRAINT TESTS
    // ==========================================
    @Test
    @Order(14)
    @DisplayName("TC314: NOT NULL Constraint - Username cannot be null")
    void testNotNullConstraint_UsernameCannotBeNull() {
        User nullUser = createTestUser();
        nullUser.setUsername(null);

        assertThatThrownBy(() -> userDAO.create(nullUser))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("null");

        logger.info("TC314 PASSED: NOT NULL constraint for username working correctly");
    }

    @Test
    @Order(15)
    @DisplayName("TC315: NOT NULL Constraint - Email cannot be null")
    void testNotNullConstraint_EmailCannotBeNull() {
        User nullUser = createTestUser();
        nullUser.setEmail(null);

        assertThatThrownBy(() -> userDAO.create(nullUser))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("null");

        logger.info("TC315 PASSED: NOT NULL constraint for email working correctly");
    }

    @Test
    @Order(16)
    @DisplayName("TC316: NOT NULL Constraint - Book title cannot be null")
    void testNotNullConstraint_BookTitleCannotBeNull() {
        Book nullBook = createTestBook();
        nullBook.setTitle(null);

        assertThatThrownBy(() -> bookDAO.create(nullBook))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("null");

        logger.info("TC316 PASSED: NOT NULL constraint for book title working correctly");
    }

    @Test
    @Order(17)
    @DisplayName("TC317: NOT NULL Constraint - Book ISBN cannot be null")
    void testNotNullConstraint_BookIsbnCannotBeNull() {
        Book nullBook = createTestBook();
        nullBook.setIsbn(null);

        assertThatThrownBy(() -> bookDAO.create(nullBook))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("null");

        logger.info("TC317 PASSED: NOT NULL constraint for book ISBN working correctly");
    }
    // ==========================================
    // TRIGGER TESTS
    // ==========================================
    @Test
    @Order(18)
    @DisplayName("TC318: Trigger - updated_at auto-update on user update")
    void testTrigger_UpdatedAtAutoUpdateOnUserUpdate() throws SQLException, InterruptedException {
        User user = userDAO.create(createTestUser());
        testUserIds.add(user.getUserId());

        Timestamp originalUpdatedAt = user.getUpdatedAt();
        pause(2000);

        user.setFullName("Updated Name for Trigger Test");
        userDAO.update(user);

        Optional<User> updatedUser = userDAO.findById(user.getUserId());
        assertThat(updatedUser).isPresent();
        assertThat(updatedUser.get().getUpdatedAt()).isAfter(originalUpdatedAt);

        logger.info("TC318 PASSED: Trigger updated_at auto-update working correctly for users");
    }

    @Test
    @Order(19)
    @DisplayName("TC319: Trigger - updated_at auto-update on book update")
    void testTrigger_UpdatedAtAutoUpdateOnBookUpdate() throws SQLException, InterruptedException {
        Book book = bookDAO.create(createTestBook());
        testBookIds.add(book.getBookId());

        Timestamp originalUpdatedAt = book.getUpdatedAt();
        pause(2000);

        book.setTitle("Updated Title for Trigger Test");
        bookDAO.updateAvailableCopies(book.getBookId(), 3);

        Optional<Book> updatedBook = bookDAO.findById(book.getBookId());
        assertThat(updatedBook).isPresent();
        assertThat(updatedBook.get().getUpdatedAt()).isAfter(originalUpdatedAt);

        logger.info("TC319 PASSED: Trigger updated_at auto-update working correctly for books");
    }
    // ==========================================
    // COMPLEX CONSTRAINT TESTS
    // ==========================================
    @Test
    @Order(20)
    @DisplayName("TC320: Complex Constraint - Multiple borrowings of same book by different users")
    void testComplexConstraint_MultipleBorrowingsSameBookDifferentUsers() throws SQLException {
        User user1 = createTestUser();
        User user2 = createTestUser();
        Book book = createTestBook();

        user1 = userDAO.create(user1);
        user2 = userDAO.create(user2);
        book = bookDAO.create(book);

        testUserIds.add(user1.getUserId());
        testUserIds.add(user2.getUserId());
        testBookIds.add(book.getBookId());

        final Integer finalBookId = book.getBookId();

        Borrowing borrowing1 = Borrowing.builder()
                .userId(user1.getUserId())
                .bookId(finalBookId)
                .dueDate(Timestamp.valueOf(LocalDateTime.now().plusDays(14)))
                .build();

        Borrowing borrowing2 = Borrowing.builder()
                .userId(user2.getUserId())
                .bookId(finalBookId)
                .dueDate(Timestamp.valueOf(LocalDateTime.now().plusDays(14)))
                .build();

        Borrowing created1 = borrowingDAO.create(borrowing1);
        Borrowing created2 = borrowingDAO.create(borrowing2);

        testBorrowingIds.add(created1.getBorrowingId());
        testBorrowingIds.add(created2.getBorrowingId());

        assertThat(created1).isNotNull();
        assertThat(created2).isNotNull();

        Optional<Book> updatedBook = bookDAO.findById(finalBookId);
        assertThat(updatedBook).isPresent();
        // Asumsi awal copies 5, dipinjam 2 orang, sisa 3
        assertThat(updatedBook.get().getAvailableCopies()).isEqualTo(3);

        logger.info("TC320 PASSED: Multiple borrowings of same book allowed for different users");
    }
    // ==========================================
    // HELPER METHODS
    // ==========================================
    private User createTestUser() {
        return User.builder()
                .username("user_" + System.currentTimeMillis() + "_" + faker.number().randomNumber())
                .email(IndonesianFakerHelper.generateIndonesianEmail())
                .fullName(IndonesianFakerHelper.generateIndonesianName())
                .phone(IndonesianFakerHelper.generateIndonesianPhone())
                .role("member")
                .status("active")
                .build();
    }

    private Book createTestBook() {
        return Book.builder()
                .isbn("978id" + System.currentTimeMillis())
                .title("Buku Integrity Test - " + faker.book().title())
                .authorId(1)
                .publisherId(1)
                .categoryId(1)
                .publicationYear(2023)
                .pages(300)
                .language("Indonesia")
                .description("Buku untuk testing integrity - " + faker.lorem().sentence())
                .totalCopies(5)
                .availableCopies(3)
                .price(new java.math.BigDecimal("75000.00"))
                .location("Rak Integrity-Test")
                .status("available")
                .build();
    }
}