package com.praktikum.database.testing.library.model;
// Import Lombok annotations
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

// Import untuk BigDecimal (harga) dan Timestamp
import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * Entity class yang merepresentasikan tabel 'books' dalam database
 * Menyimpan informasi tentang buku yang ada di perpustakaan
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Book {
    // Primary key dari tabel books
    private Integer bookId;

    // ISBN unik untuk identifikasi buku
    private String isbn;

    // Judul buku
    private String title;

    // Foreign key ke tabel authors
    private Integer authorId;

    // Foreign key ke tabel publishers (bisa null)
    private Integer publisherId;

    // Foreign key ke tabel categories (bisa null)
    private Integer categoryId;

    // Tahun publikasi buku
    private Integer publicationYear;

    // Jumlah halaman buku
    private Integer pages;

    // Bahasa buku (default: Indonesian)
    private String language;

    // Deskripsi atau sinopsis buku
    private String description;

    // Total jumlah kopi buku yang dimiliki
    private Integer totalCopies;

    // Jumlah kopi yang tersedia untuk dipinjam
    private Integer availableCopies;

    // Harga buku (optional)
    private BigDecimal price;

    // Lokasi fisik buku di perpustakaan
    private String location;

    // Status buku: available, unavailable, maintenance
    private String status;

    // Timestamp ketika record dibuat
    private Timestamp createdAt;

    // Timestamp ketika record terakhir diupdate
    private Timestamp updatedAt;
}