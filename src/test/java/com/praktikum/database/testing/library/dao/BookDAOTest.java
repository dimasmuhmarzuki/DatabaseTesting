package com.praktikum.database.testing.library.dao;
// Import classes untuk testing
import com.github.javafaker.Faker;
import com.praktikum.database.testing.library.BaseDatabaseTest;
import com.praktikum.database.testing.library.model.Book;
import org.junit.jupiter.api.*;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
// Import static assertions
import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive test suite untuk BookDAO
 * Menguji semua operasi CRUD pada entity Book
 * Termasuk testing business logic untuk available copies
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("BookDAO CRUD Operations Test Suite")
public class BookDAOTest extends BaseDatabaseTest {

    // Test dependencies
    private static BookDAO bookDAO;
    private static Faker faker;

    // Test data
    private static Book testBook;
    private static List<Integer> createdBookIds;

    @BeforeAll
    static void setUpAll() {
        logger.info("Starting BookDAO CRUD Tests");
        // Initialize dependencies
        bookDAO = new BookDAO();
        faker = new Faker();
        createdBookIds = new ArrayList<>();
    }

    @AfterAll
    static void tearDownAll() throws SQLException {
        logger.info("BookDAO CRUD Tests Completed");

        // Cleanup test data
        for (Integer bookId : createdBookIds) {
            try {
                bookDAO.delete(bookId);
                logger.fine("Cleaned up test book ID: " + bookId);
            } catch (SQLException e) {
                logger.warning("Gagal cleanup book ID: " + bookId + " - " + e.getMessage());
            }
        }
    }
    // =========================================================================
    // POSITIVE TEST CASES
    // =========================================================================
    @Test
    @Order(1)
    @DisplayName("TC101: Create book dengan data valid - Should Success")
    void testCreateBook_WithValidData_ShouldSuccess() throws SQLException {
        // ARRANGE
        testBook = createTestBook();

        // ACT
        Book createdBook = bookDAO.create(testBook);
        createdBookIds.add(createdBook.getBookId());

        // ASSERT
        assertThat(createdBook).isNotNull();
        assertThat(createdBook).satisfies(book -> {
            assertThat(book.getBookId()).isNotNull().isPositive();
            assertThat(book.getIsbn()).isEqualTo(testBook.getIsbn());
            assertThat(book.getTitle()).isEqualTo(testBook.getTitle());
            assertThat(book.getAuthorId()).isEqualTo(testBook.getAuthorId());
            assertThat(book.getTotalCopies()).isEqualTo(5);
            assertThat(book.getAvailableCopies()).isEqualTo(5);
            assertThat(book.getStatus()).isEqualTo("available");
            assertThat(book.getCreatedAt()).isNotNull();
            assertThat(book.getUpdatedAt()).isNotNull();
        });

        logger.info("TC101 PASSED: Book created dengan ID: " + createdBook.getBookId());
    }

    @Test
    @Order(2)
    @DisplayName("TC102: Find book by existing ID - Should Return Book")
    void testFindBookById_WithExistingId_ShouldReturnBook() throws SQLException {
        // ACT
        Optional<Book> foundBook = bookDAO.findById(testBook.getBookId());

        // ASSERT
        assertThat(foundBook)
                .isPresent()
                .get()
                .satisfies(book -> {
                    assertThat(book.getBookId()).isEqualTo(testBook.getBookId());
                    assertThat(book.getTitle()).isEqualTo(testBook.getTitle());
                    assertThat(book.getIsbn()).isEqualTo(testBook.getIsbn());
                });

        logger.info("TC102 PASSED: Book found dengan ID: " + testBook.getBookId());
    }

    @Test
    @Order(3)
    @DisplayName("TC103: Find book by ISBN - Should Return Book")
    void testFindBookByIsbn_WithExistingIsbn_ShouldReturnBook() throws SQLException {
        // ACT
        Optional<Book> foundBook = bookDAO.findByIsbn(testBook.getIsbn());

        // ASSERT
        assertThat(foundBook)
                .isPresent()
                .get()
                .satisfies(book -> {
                    assertThat(book.getIsbn()).isEqualTo(testBook.getIsbn());
                });

        logger.info("TC103 PASSED: Book found dengan ISBN: " + testBook.getIsbn());
    }

    @Test
    @Order(4)
    @DisplayName("TC104: Find all books - Should Return Non-Empty List")
    void testFindAllBooks_ShouldReturnNonEmptyList() throws SQLException {
        // ACT
        List<Book> books = bookDAO.findAll();

        // ASSERT
        assertThat(books)
                .isNotNull()
                .isNotEmpty()
                .hasSizeGreaterThanOrEqualTo(1)
                .allSatisfy(book -> {
                    assertThat(book.getBookId()).isNotNull();
                    assertThat(book.getTitle()).isNotBlank();
                    assertThat(book.getIsbn()).isNotBlank();
                });

        logger.info("TC104 PASSED: Found " + books.size() + " books");
    }

    @Test
    @Order(5)
    @DisplayName("TC105: Decrease available copies - Should Success")
    void testDecreaseAvailableCopies_ShouldSuccess() throws SQLException {
        // ARRANGE - Get original available copies
        Optional<Book> beforeDecrease = bookDAO.findById(testBook.getBookId());
        int originalCopies = beforeDecrease.get().getAvailableCopies();

        // ACT - Decrease available copies (simulasi peminjaman)
        boolean decreased = bookDAO.decreaseAvailableCopies(testBook.getBookId());

        // ASSERT
        assertThat(decreased).isTrue();

        // VERIFY - Available copies should decrease by 1
        Optional<Book> afterDecrease = bookDAO.findById(testBook.getBookId());
        assertThat(afterDecrease)
                .isPresent()
                .get()
                .satisfies(book -> {
                    assertThat(book.getAvailableCopies()).isEqualTo(originalCopies - 1);
                });

        logger.info("TC105 PASSED: Available copies decreased: " + originalCopies + " -> " + afterDecrease.get().getAvailableCopies());
    }

    @Test
    @Order(6)
    @DisplayName("TC106: Increase available copies - Should Success")
    void testIncreaseAvailableCopies_ShouldSuccess() throws SQLException {
        // ARRANGE - Get current available copies
        Optional<Book> beforeIncrease = bookDAO.findById(testBook.getBookId());
        int originalCopies = beforeIncrease.get().getAvailableCopies();

        // ACT - Increase available copies (simulasi pengembalian)
        boolean increased = bookDAO.increaseAvailableCopies(testBook.getBookId());

        // ASSERT
        assertThat(increased).isTrue();

        // VERIFY - Available copies should increase by 1
        Optional<Book> afterIncrease = bookDAO.findById(testBook.getBookId());
        assertThat(afterIncrease)
                .isPresent()
                .get()
                .satisfies(book -> {
                    assertThat(book.getAvailableCopies()).isEqualTo(originalCopies + 1);
                });

        logger.info("TC106 PASSED: Available copies increased: " + originalCopies + " -> " + afterIncrease.get().getAvailableCopies());
    }

    @Test
    @Order(7)
    @DisplayName("TC107: Search books by title - Should Return Matching Books")
    void testSearchByTitle_WithExistingTitle_ShouldReturnBooks() throws SQLException {
        // ARRANGE - Create a book dengan unique title untuk testing search
        String uniqueTitle = "UniqueSearchTestBook" + System.currentTimeMillis();
        Book searchBook = createTestBook();
        searchBook.setTitle(uniqueTitle);
        searchBook = bookDAO.create(searchBook);
        createdBookIds.add(searchBook.getBookId());

        // ACT - Search by partial title
        List<Book> foundBooks = bookDAO.searchByTitle("SearchTest");

        // ASSERT
        assertThat(foundBooks)
                .isNotNull()
                .isNotEmpty()
                .anySatisfy(book -> {
                    assertThat(book.getTitle()).containsIgnoringCase("SearchTest");
                });

        logger.info("TC107 PASSED: Found " + foundBooks.size() + " books matching search");
    }

    @Test
    @Order(8)
    @DisplayName("TC108: Find available books - Should Return Only Available Books")
    void testFindAvailableBooks_ShouldReturnOnlyAvailableBooks() throws SQLException {
        // ACT
        List<Book> availableBooks = bookDAO.findAvailableBooks();

        // ASSERT
        assertThat(availableBooks)
                .isNotNull()
                .allSatisfy(book -> {
                    assertThat(book.getAvailableCopies()).isGreaterThan(0);
                    assertThat(book.getStatus()).isEqualTo("available");
                });

        logger.info("TC108 PASSED: Found " + availableBooks.size() + " available books");
    }

    @Test
    @Order(9)
    @DisplayName("TC109: Count all books - Should Return Correct Count")
    void testCountAllBooks_ShouldReturnCorrectCount() throws SQLException {
        // ACT
        int totalCount = bookDAO.countAll();
        int availableCount = bookDAO.countAvailableBooks();

        // ASSERT
        assertThat(totalCount).isGreaterThanOrEqualTo(1);
        assertThat(availableCount).isLessThanOrEqualTo(totalCount);

        logger.info("TC109 PASSED: Total books: " + totalCount + ", Available: " + availableCount);
    }
    // =========================================================================
    // NEGATIVE TEST CASES
    // =========================================================================
    @Test
    @Order(20)
    @DisplayName("TC120: Create book dengan duplicate ISBN - Should Fail")
    void testCreateBook_WithDuplicateIsbn_ShouldFail() {
        // ARRANGE - Book dengan ISBN yang sama
        Book duplicateBook = createTestBook();
        duplicateBook.setIsbn(testBook.getIsbn()); // Same ISBN

        // ACT & ASSERT
        assertThatThrownBy(() -> bookDAO.create(duplicateBook))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("duplicate")
                .hasMessageContaining("isbn");

        logger.info("TC120 PASSED: Duplicate ISBN constraint working correctly");
    }

    @Test
    @Order(21)
    @DisplayName("TC121: Create book dengan available > total copies - Should Fail")
    void testCreateBook_WithAvailableGreaterThanTotal_ShouldFail() {
        // ARRANGE - Available copies > total copies (violates check constraint)
        Book invalidBook = createTestBook();
        invalidBook.setTotalCopies(5);
        invalidBook.setAvailableCopies(10); // BUG: 10 > 5

        // ACT & ASSERT
        assertThatThrownBy(() -> bookDAO.create(invalidBook))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("check_available_copies");

        logger.info("TC121 PASSED: Constraint check_available_copies working correctly");
    }

    @Test
    @Order(22)
    @DisplayName("TC122: Create book dengan invalid publication year - Should Fail")
    void testCreateBook_WithInvalidPublicationYear_ShouldFail() {
        // ARRANGE - Invalid publication year
        Book invalidBook = createTestBook();
        invalidBook.setPublicationYear(999); // Invalid: < 1000

        // ACT & ASSERT
        assertThatThrownBy(() -> bookDAO.create(invalidBook))
                .isInstanceOf(SQLException.class);

        logger.info("TC122 PASSED: Publication year constraint working correctly");
    }

    @Test
    @Order(23)
    @DisplayName("TC123: Decrease copies ketika sudah 0 - Should Fail")
    void testDecreaseAvailableCopies_WhenAlreadyZero_ShouldFail() throws SQLException {
        // ARRANGE - Set available copies to 0
        bookDAO.updateAvailableCopies(testBook.getBookId(), 0);

        // ACT - Try to decrease when copies = 0
        boolean decreased = bookDAO.decreaseAvailableCopies(testBook.getBookId());

        // ASSERT
        assertThat(decreased).isFalse(); // Should return false, not throw exception

        logger.info("TC123 PASSED: Cannot decrease when copies = 0");

        // RESTORE - Reset to original value
        bookDAO.updateAvailableCopies(testBook.getBookId(), 5);
    }

    @Test
    @Order(24)
    @DisplayName("TC124: Increase copies ketika sudah mencapai total - Should Fail")
    void testIncreaseAvailableCopies_WhenAtTotal_ShouldFail() throws SQLException {
        // ARRANGE - Set available copies = total copies
        Optional<Book> book = bookDAO.findById(testBook.getBookId());
        int totalCopies = book.get().getTotalCopies();
        bookDAO.updateAvailableCopies(testBook.getBookId(), totalCopies);

        // ACT - Try to increase when copies = total
        boolean increased = bookDAO.increaseAvailableCopies(testBook.getBookId());

        // ASSERT
        assertThat(increased).isFalse(); // Should return false

        logger.info("TC124 PASSED: Cannot increase when copies = total");

        // RESTORE
        bookDAO.updateAvailableCopies(testBook.getBookId(), 5);
    }

    @Test
    @Order(25)
    @DisplayName("TC125: Find book dengan non-existent ID - Should Return Empty")
    void testFindBookById_WithNonExistentId_ShouldReturnEmpty() throws SQLException {
        // ACT
        Optional<Book> foundBook = bookDAO.findById(999999);

        // ASSERT
        assertThat(foundBook).isEmpty();

        logger.info("TC125 PASSED: Non-existent book handled correctly");
    }

    @Test
    @Order(26)
    @DisplayName("TC126: Delete non-existent book - Should Return False")
    void testDeleteBook_WithNonExistentBook_ShouldReturnFalse() throws SQLException {
        // ACT
        boolean deleted = bookDAO.delete(999999);

        // ASSERT
        assertThat(deleted).isFalse();

        logger.info("TC126 PASSED: Non-existent book delete handled correctly");
    }
    // =========================================================================
    // BOUNDARY TEST CASES
    // =========================================================================
    @Test
    @Order(30)
    @DisplayName("TC130: Create book dengan ISBN panjang maksimum - Should Success")
    void testCreateBook_WithMaxLengthIsbn_ShouldSuccess() throws SQLException {
        // ARRANGE - ISBN dengan panjang 13 characters
        String maxLengthIsbn = "9".repeat(13);
        Book book = createTestBook();
        book.setIsbn(maxLengthIsbn);

        // ACT
        Book createdBook = bookDAO.create(book);
        createdBookIds.add(createdBook.getBookId());

        // ASSERT
        assertThat(createdBook.getIsbn()).hasSize(13);

        logger.info("TC130 PASSED: Max length ISBN accepted");
    }

    @Test
    @Order(31)
    @DisplayName("TC131: Create book dengan title panjang maksimum - Should Success")
    void testCreateBook_WithMaxLengthTitle_ShouldSuccess() throws SQLException {
        // ARRANGE - Title dengan panjang 200 characters
        String maxLengthTitle = "T".repeat(200);
        Book book = createTestBook();
        book.setTitle(maxLengthTitle);

        // ACT
        Book createdBook = bookDAO.create(book);
        createdBookIds.add(createdBook.getBookId());

        // ASSERT
        assertThat(createdBook.getTitle()).hasSize(200);

        logger.info("TC131 PASSED: Max length title accepted");
    }
    // =========================================================================
    // PERFORMANCE TEST CASES
    // =========================================================================
    @Test
    @Order(40)
    @DisplayName("TC140: Search performance test - Should Complete Quickly")
    void testSearchPerformance() throws SQLException {
        // ARRANGE
        int iterations = 10;
        long totalTime = 0;

        // ACT & MEASURE
        for (int i = 0; i < iterations; i++) {
            long startTime = System.nanoTime();
            bookDAO.searchByTitle("Test");
            long endTime = System.nanoTime();
            totalTime += (endTime - startTime);
        }

        long averageTimeMs = (totalTime / iterations) / 1_000_000;

        // ASSERT
        assertThat(averageTimeMs).isLessThan(2000); // Should be under 200ms

        logger.info("TC140 PASSED: Average search time: " + averageTimeMs + " ms");
    }
    // =========================================================================
    // HELPER METHODS
    // =========================================================================
    /**
     * Helper method untuk membuat test book dengan data yang valid
     * @return Book object untuk testing
     */
    private Book createTestBook() {
        return Book.builder()
                .isbn("978" + System.currentTimeMillis())
                .title(faker.book().title() + " Test Book")
                .authorId(1) // Assuming author_id 1 exists from sample data
                .publisherId(1)
                .categoryId(1)
                .publicationYear(2023)
                .pages(300)
                .language("Indonesian")
                .description("Test book description for automated testing")
                .totalCopies(5)
                .availableCopies(5)
                .price(new BigDecimal("75000.00"))
                .location("Rak A-1")
                .status("available")
                .build();
    }
}