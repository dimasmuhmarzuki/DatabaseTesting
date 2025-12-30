package com.praktikum.database.testing.library.model;
// Import Lombok annotations
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
// Import untuk BigDecimal (denda) dan Timestamp
import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * Entity class yang merepresentasikan tabel 'borrowings' dalam database
 * Menyimpan informasi tentang peminjaman buku oleh user
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Borrowing {
    // Primary key dari tabel borrowings
    private Integer borrowingId;

    // Foreign key ke tabel users (siapa yang meminjam)
    private Integer userId;

    // Foreign key ke tabel books (buku yang dipinjam)
    private Integer bookId;

    // Tanggal buku dipinjam (auto-generated)
    private Timestamp borrowDate;

    // Tanggal batas pengembalian (harus di-set)
    private Timestamp dueDate;

    // Tanggal buku dikembalikan (null jika belum dikembalikan)
    private Timestamp returnDate;

    // Status peminjaman: borrowed, returned, overdue, lost
    private String status;

    // Jumlah denda yang harus dibayar
    private BigDecimal fineAmount;

    // Flag apakah denda sudah dibayar
    private Boolean finePaid;

    // Catatan tambahan tentang peminjaman
    private String notes;

    // Timestamp ketika record dibuat
    private Timestamp createdAt;

    // Timestamp ketika record terakhir diupdate
    private Timestamp updatedAt;
}