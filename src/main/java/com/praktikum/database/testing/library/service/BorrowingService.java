package com.praktikum.database.testing.library.service;
// Import DAO classes dan model classes
import com.praktikum.database.testing.library.dao.BookDAO;
import com.praktikum.database.testing.library.dao.BorrowingDAO;
import com.praktikum.database.testing.library.dao.UserDAO;
import com.praktikum.database.testing.library.model.Book;
import com.praktikum.database.testing.library.model.Borrowing;
import com.praktikum.database.testing.library.model.User;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * Service class yang mengelola business logic untuk peminjaman buku
 * Mengintegrasikan UserDAO, BookDAO, dan BorrowingDAO
 * Menangani validasi business rules dan transaction logic
 */
public class BorrowingService {
    // Logger untuk mencatat aktivitas service
    private static final Logger logger = Logger.getLogger(BorrowingService.class.getName());

    // DAO dependencies
    private final UserDAO userDAO;
    private final BookDAO bookDAO;
    private final BorrowingDAO borrowingDAO;

    /**
     * Constructor default - initialize semua DAO
     */
    public BorrowingService() {
        this.userDAO = new UserDAO();
        this.bookDAO = new BookDAO();
        this.borrowingDAO = new BorrowingDAO();
    }

    /**
     * Constructor dengan dependency injection (untuk testing)
     * @param userDAO UserDAO instance
     * @param bookDAO BookDAO instance
     * @param borrowingDAO BorrowingDAO instance
     */
    public BorrowingService(UserDAO userDAO, BookDAO bookDAO, BorrowingDAO borrowingDAO) {
        this.userDAO = userDAO;
        this.bookDAO = bookDAO;
        this.borrowingDAO = borrowingDAO;
    }

    /**
     * Memproses peminjaman buku dengan validasi lengkap
     * @param userId ID user yang meminjam
     * @param bookId ID buku yang dipinjam
     * @param borrowDays Jumlah hari peminjaman
     * @return Borrowing object yang berhasil dibuat
     * @throws SQLException jika operasi database gagal
     * @throws IllegalArgumentException jika data tidak valid
     * @throws IllegalStateException jika business rules dilanggar
     */
    public Borrowing borrowBook(Integer userId, Integer bookId, int borrowDays) throws SQLException {
        logger.info("Memproses peminjaman buku - User: " + userId + ", Book: " + bookId);

        // STEP 1: Validasi user exists dan active
        Optional<User> user = userDAO.findById(userId);
        if (user.isEmpty()) {
            logger.warning("User tidak ditemukan dengan ID: " + userId);
            throw new IllegalArgumentException("User tidak ditemukan dengan ID: " + userId);
        }

        // STEP 2: Validasi user status harus active
        if (!"active".equals(user.get().getStatus())) {
            logger.warning("User account tidak active. Status: " + user.get().getStatus());
            throw new IllegalStateException("User account tidak active. Status: " + user.get().getStatus());
        }

        // STEP 3: Validasi book exists
        Optional<Book> book = bookDAO.findById(bookId);
        if (book.isEmpty()) {
            logger.warning("Buku tidak ditemukan dengan ID: " + bookId);
            throw new IllegalArgumentException("Buku tidak ditemukan dengan ID: " + bookId);
        }

        // STEP 4: Validasi book available untuk dipinjam
        if (book.get().getAvailableCopies() <= 0) {
            logger.warning("Tidak ada kopi yang tersedia untuk buku ini");
            throw new IllegalStateException("Tidak ada kopi yang tersedia untuk buku ini");
        }

        // STEP 5: Validasi user tidak melebihi batas peminjaman
        int activeBorrowings = borrowingDAO.countActiveBorrowingsByUser(userId);
        if (activeBorrowings >= 5) { // Batas maksimal 5 buku
            logger.warning("User sudah mencapai batas peminjaman: " + activeBorrowings + " buku");
            throw new IllegalStateException("User sudah mencapai batas peminjaman: " + activeBorrowings + " buku");
        }

        // STEP 6: Decrease available copies di buku
        boolean decreased = bookDAO.decreaseAvailableCopies(bookId);
        if (!decreased) {
            logger.severe("Gagal mengurangi available copies untuk buku ID: " + bookId);
            throw new IllegalStateException("Gagal mengurangi available copies");
        }

        // STEP 7: Create borrowing record
        Timestamp dueDate = Timestamp.valueOf(LocalDateTime.now().plusDays(borrowDays));
        Borrowing borrowing = Borrowing.builder()
                .userId(userId)
                .bookId(bookId)
                .dueDate(dueDate)
                .status("borrowed")
                .notes("Dipinjam via BorrowingService - " + borrowDays + " hari")
                .build();

        Borrowing createdBorrowing = borrowingDAO.create(borrowing);
        logger.info("Peminjaman berhasil - Borrowing ID: " + createdBorrowing.getBorrowingId());
        return createdBorrowing;
    }

    /**
     * Memproses pengembalian buku
     * @param borrowingId ID borrowing record yang akan dikembalikan
     * @return true jika pengembalian berhasil
     * @throws SQLException jika operasi database gagal
     */
    public boolean returnBook(Integer borrowingId) throws SQLException {
        logger.info("Memproses pengembalian buku - Borrowing ID: " + borrowingId);

        // STEP 1: Validasi borrowing exists
        Optional<Borrowing> borrowing = borrowingDAO.findById(borrowingId);
        if (borrowing.isEmpty()) {
            logger.warning("Borrowing record tidak ditemukan dengan ID: " + borrowingId);
            throw new IllegalArgumentException("Borrowing record tidak ditemukan dengan ID: " + borrowingId);
        }

        // STEP 2: Validasi buku belum dikembalikan
        if (borrowing.get().getReturnDate() != null) {
            logger.warning("Buku sudah dikembalikan sebelumnya");
            throw new IllegalStateException("Buku sudah dikembalikan");
        }

        // STEP 3: Update return date di borrowing record
        Timestamp returnDate = new Timestamp(System.currentTimeMillis());
        boolean updated = borrowingDAO.returnBook(borrowingId, returnDate);

        if (!updated) {
            logger.severe("Gagal update return date untuk borrowing ID: " + borrowingId);
            throw new IllegalStateException("Gagal update return date");
        }

        // STEP 4: Increase available copies di buku
        Integer bookId = borrowing.get().getBookId();
        boolean increased = bookDAO.increaseAvailableCopies(bookId);
        if (!increased) {
            logger.severe("Gagal menambah available copies untuk buku ID: " + bookId);
            throw new IllegalStateException("Gagal menambah available copies");
        }

        logger.info("Pengembalian berhasil - Book ID: " + bookId);
        return true;
    }

    /**
     * Memeriksa apakah user bisa meminjam buku tertentu
     * @param userId ID user yang akan meminjam
     * @param bookId ID buku yang akan dipinjam
     * @return true jika user bisa meminjam buku tersebut
     * @throws SQLException jika operasi database gagal
     */
    public boolean canUserBorrowBook(Integer userId, Integer bookId) throws SQLException {
        // STEP 1: Check user exists dan active
        Optional<User> user = userDAO.findById(userId);
        if (user.isEmpty() || !"active".equals(user.get().getStatus())) {
            return false;
        }

        // STEP 2: Check book exists dan available
        Optional<Book> book = bookDAO.findById(bookId);
        if (book.isEmpty() || book.get().getAvailableCopies() <= 0) {
            return false;
        }

        // STEP 3: Check user tidak melebihi batas peminjaman
        int activeBorrowings = borrowingDAO.countActiveBorrowingsByUser(userId);
        if (activeBorrowings >= 5) {
            return false;
        }

        return true;
    }

    /**
     * Menghitung denda untuk borrowing yang overdue
     * @param borrowingId ID borrowing record
     * @return jumlah denda yang harus dibayar
     * @throws SQLException jika operasi database gagal
     */
    public double calculateFine(Integer borrowingId) throws SQLException {
        Optional<Borrowing> borrowing = borrowingDAO.findById(borrowingId);
        if (borrowing.isEmpty()) {
            throw new IllegalArgumentException("Borrowing record tidak ditemukan");
        }

        Borrowing borrow = borrowing.get();

        // Jika sudah dikembalikan, return existing fine amount
        if (borrow.getReturnDate() != null) {
            return borrow.getFineAmount() != null ? borrow.getFineAmount().doubleValue() : 0.0;
        }

        // Jika belum overdue, tidak ada denda
        if (borrow.getDueDate().after(new Timestamp(System.currentTimeMillis()))) {
            return 0.0;
        }

        // Hitung denda: 5000 per hari keterlambatan
        long overdueDays = (System.currentTimeMillis() - borrow.getDueDate().getTime()) / (1000 * 60 * 60 * 24);
        double fine = overdueDays * 5000.0; // 5000 per hari

        logger.info("Denda dihitung - Borrowing ID: " + borrowingId + ", Denda: " + fine);
        return fine;
    }

    /**
     * Mendapatkan active borrowings untuk user tertentu
     * @param userId ID user
     * @return List of active borrowing records
     * @throws SQLException jika operasi database gagal
     */
    public java.util.List<Borrowing> getUserActiveBorrowings(Integer userId) throws SQLException {
        return borrowingDAO.findByUserId(userId).stream()
                .filter(borrowing -> borrowing.getReturnDate() == null)
                .toList();
    }

    /**
     * Mendapatkan borrowing history untuk user tertentu
     * @param userId ID user
     * @return List of semua borrowing records user
     * @throws SQLException jika operasi database gagal
     */
    public java.util.List<Borrowing> getUserBorrowingHistory(Integer userId) throws SQLException {
        return borrowingDAO.findByUserId(userId);
    }

    /**
     * Update status borrowing menjadi overdue jika melewati due_date
     * Method ini bisa dijalankan periodically (e.g., via scheduler)
     * @throws SQLException jika operasi database gagal
     */
    public void updateOverdueStatus() throws SQLException {
        logger.info("Memperbarui status overdue borrowings...");

        java.util.List<Borrowing> overdueBorrowings = borrowingDAO.findOverdueBorrowings();

        for (Borrowing borrowing : overdueBorrowings) {
            if (!"overdue".equals(borrowing.getStatus())) {
                borrowingDAO.updateStatus(borrowing.getBorrowingId(), "overdue");

                // Calculate dan update fine amount
                double fine = calculateFine(borrowing.getBorrowingId());
                borrowingDAO.updateFineAmount(borrowing.getBorrowingId(), fine);

                logger.info("Updated to overdue - Borrowing ID: " + borrowing.getBorrowingId() + ", Fine: " + fine);
            }
        }

        logger.info("Overdue status update completed - Total: " + overdueBorrowings.size());
    }
}