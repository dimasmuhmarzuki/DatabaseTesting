package com.praktikum.database.testing.library.integration;

import com.github.javafaker.Faker;
import com.praktikum.database.testing.library.BaseDatabaseTest;
import com.praktikum.database.testing.library.config.DatabaseConfig;
import com.praktikum.database.testing.library.dao.BookDAO;
import com.praktikum.database.testing.library.dao.BorrowingDAO;
import com.praktikum.database.testing.library.dao.UserDAO;
import com.praktikum.database.testing.library.model.Book;
import com.praktikum.database.testing.library.model.Borrowing;
import com.praktikum.database.testing.library.model.User;
import com.praktikum.database.testing.library.service.BorrowingService;
import com.praktikum.database.testing.library.utils.IndonesianFakerHelper;
import org.junit.jupiter.api.*;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import static org.assertj.core.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Borrowing Integration Test Suite")
public class BorrowingIntegrationTest extends BaseDatabaseTest {

    private static final Logger logger = Logger.getLogger(BorrowingIntegrationTest.class.getName());
    private static UserDAO userDAO;
    private static BookDAO bookDAO;
    private static BorrowingDAO borrowingDAO;
    private static BorrowingService borrowingService;
    private static Faker faker;
    private static User testUser;
    private static Book testBook;

    @BeforeAll
    static void setUpAll() {
        userDAO = new UserDAO();
        bookDAO = new BookDAO();
        borrowingDAO = new BorrowingDAO();
        borrowingService = new BorrowingService(userDAO, bookDAO, borrowingDAO);
        faker = IndonesianFakerHelper.getFaker();
    }

    @BeforeEach
    void setUp() throws SQLException {
        setupTestData();
    }

    @AfterEach
    void tearDown() throws SQLException {
        cleanupTestData();
    }

    private void setupTestData() throws SQLException {
        testUser = userDAO.create(User.builder()
                .username("integ_" + System.currentTimeMillis())
                .email(IndonesianFakerHelper.generateIndonesianEmail())
                .fullName(IndonesianFakerHelper.generateIndonesianName())
                .phone(IndonesianFakerHelper.generateIndonesianPhone())
                .role("member").status("active").build());

        testBook = bookDAO.create(Book.builder()
                .isbn("978" + System.currentTimeMillis())
                .title("Buku Test " + faker.book().title())
                .authorId(1).totalCopies(5).availableCopies(5)
                .price(new BigDecimal("85000")).language("Indonesia").build());
    }

    private void cleanupTestData() throws SQLException {
        if (testUser == null || testUser.getUserId() == null) return;
        List<Borrowing> borrs = borrowingDAO.findByUserId(testUser.getUserId());
        for (Borrowing b : borrs) borrowingDAO.delete(b.getBorrowingId());
        if (testBook != null) bookDAO.delete(testBook.getBookId());
        userDAO.delete(testUser.getUserId());
    }

    @Test
    @Order(1)
    void testCompleteBorrowingWorkflow_SuccessScenario() throws SQLException {
        Borrowing b = borrowingService.borrowBook(testUser.getUserId(), testBook.getBookId(), 14);
        assertThat(b).isNotNull();
        assertThat(b.getStatus()).isEqualTo("borrowed");
    }

    @Test
    @Order(2)
    void testCompleteReturnWorkflow_SuccessScenario() throws Exception {
        Borrowing borrowing = borrowingService.borrowBook(testUser.getUserId(), testBook.getBookId(), 14);
        Thread.sleep(2000); // Jeda agar return_date > borrow_date
        boolean returned = borrowingService.returnBook(borrowing.getBorrowingId());
        assertThat(returned).isTrue();
    }

    @Test
    @Order(3)
    void testBorrowBook_WithInactiveUser_ShouldFail() throws SQLException {
        testUser.setStatus("inactive");
        userDAO.update(testUser);
        assertThatThrownBy(() -> borrowingService.borrowBook(testUser.getUserId(), testBook.getBookId(), 14))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @Order(4)
    void testBorrowBook_UnavailableBook_ShouldFail() throws SQLException {
        bookDAO.updateAvailableCopies(testBook.getBookId(), 0);
        assertThatThrownBy(() -> borrowingService.borrowBook(testUser.getUserId(), testBook.getBookId(), 14))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Tidak ada kopi");
    }

    @Test
    @Order(5)
    void testReturnBook_AlreadyReturned_ShouldFail() throws Exception {
        Borrowing b = borrowingService.borrowBook(testUser.getUserId(), testBook.getBookId(), 14);
        Thread.sleep(2000);
        borrowingService.returnBook(b.getBorrowingId());

        assertThatThrownBy(() -> borrowingService.returnBook(b.getBorrowingId()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @Order(6)
    void testMultipleBorrowings_BySameUser_ShouldSuccess() throws Exception {
        Book b2 = bookDAO.create(createTestBook());
        Borrowing br1 = borrowingService.borrowBook(testUser.getUserId(), testBook.getBookId(), 14);
        Borrowing br2 = borrowingService.borrowBook(testUser.getUserId(), b2.getBookId(), 7);

        Thread.sleep(2000);
        borrowingService.returnBook(br1.getBorrowingId());
        borrowingService.returnBook(br2.getBorrowingId());
        bookDAO.delete(b2.getBookId());
    }

    @Test
    @Order(7)
    void testBorrowingLimitEnforcement_MaximumFiveBooks() throws SQLException {
        for (int i = 0; i < 5; i++) {
            Book b = bookDAO.create(createTestBook());
            borrowingService.borrowBook(testUser.getUserId(), b.getBookId(), 14);
        }
        assertThatThrownBy(() -> borrowingService.borrowBook(testUser.getUserId(), testBook.getBookId(), 14))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("batas peminjaman");
    }

    @Test
    @Order(8)
    void testConcurrentBorrowingSimulation_RaceConditionHandling() throws SQLException {
        bookDAO.updateAvailableCopies(testBook.getBookId(), 1);
        borrowingService.borrowBook(testUser.getUserId(), testBook.getBookId(), 14);
        assertThatThrownBy(() -> borrowingService.borrowBook(testUser.getUserId(), testBook.getBookId(), 14))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Tidak ada kopi");
    }

    @Test
    @Order(9)
    void testDataConsistency_AfterMultipleOperations() throws Exception {
        Borrowing b1 = borrowingService.borrowBook(testUser.getUserId(), testBook.getBookId(), 14);
        Thread.sleep(2000);
        borrowingService.returnBook(b1.getBorrowingId());

        Optional<Book> fb = bookDAO.findById(testBook.getBookId());
        assertThat(fb.get().getAvailableCopies()).isEqualTo(5);
    }

    @Test
    @Order(10)
    void testFineCalculation_ForOverdueBooks() throws SQLException {
        // 1. Buat data peminjaman yang 'valid' di mata database (due_date masa depan)
        // agar tidak melanggar check constraint saat proses INSERT
        long cur = System.currentTimeMillis();
        Timestamp futureDue = new Timestamp(cur + (7L * 24 * 60 * 60 * 1000)); // +7 hari

        Borrowing overdue = Borrowing.builder()
                .userId(testUser.getUserId())
                .bookId(testBook.getBookId())
                .dueDate(futureDue)
                .status("borrowed")
                .build();

        // Simpan menggunakan DAO asli tanpa perubahan
        Borrowing saved = borrowingDAO.create(overdue);
        Integer borrowingId = saved.getBorrowingId();

        // 2. Lakukan 'Backdating' manual menggunakan JDBC biasa di dalam Test
        // Kita paksa tanggalnya mundur agar menjadi overdue (telat)
        long dayInMillis = 24 * 60 * 60 * 1000L;
        Timestamp borrowPast = new Timestamp(cur - (10L * dayInMillis)); // Pinjam 10 hari lalu
        Timestamp duePast = new Timestamp(cur - (5L * dayInMillis));    // Harus balik 5 hari lalu

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "UPDATE borrowings SET borrow_date = ?, due_date = ? WHERE borrowing_id = ?")) {
            pstmt.setTimestamp(1, borrowPast);
            pstmt.setTimestamp(2, duePast);
            pstmt.setInt(3, borrowingId);
            pstmt.executeUpdate();
        }

        // 3. Sekarang panggil service untuk hitung denda
        // Karena di DB tanggalnya sudah mundur, kalkulasi denda pasti > 0
        double fine = borrowingService.calculateFine(borrowingId);

        assertThat(fine).isGreaterThan(0);
    }

    @Test
    @Order(11)
    @DisplayName("TC411: Transaction integrity - All or nothing principle")
    void testTransactionIntegrity_AllOrNothingPrinciple() throws SQLException {
        // Simpan jumlah stok awal
        int initial = testBook.getAvailableCopies();

        // Mencoba meminjam dengan User ID yang tidak terdaftar (999999)
        // Hal ini seharusnya memicu kegagalan di level Service atau DAO
        try {
            borrowingService.borrowBook(999999, testBook.getBookId(), 14);
        } catch (Exception ignored) {
            // Exception diharapkan terjadi karena User ID tidak valid
        }

        // Pastikan stok buku TIDAK berkurang (Rollback mechanism)
        // Jika transaksi tidak atomik, stok mungkin berkurang meskipun peminjaman gagal
        assertThat(bookDAO.findById(testBook.getBookId()).get().getAvailableCopies()).isEqualTo(initial);
    }

    @Test
    @Order(12)
    void testServiceLayerValidation_InvalidParameters() {
        assertThatThrownBy(() -> borrowingService.borrowBook(null, testBook.getBookId(), 14)).isInstanceOf(Exception.class);
        assertThatThrownBy(() -> borrowingService.borrowBook(testUser.getUserId(), null, 14)).isInstanceOf(Exception.class);
    }

    private Book createTestBook() {
        return Book.builder().isbn("978" + System.nanoTime()).title("Buku " + faker.funnyName().name())
                .authorId(1).totalCopies(1).availableCopies(1).price(new BigDecimal("50000")).language("ID").build();
    }
}