package com.praktikum.database.testing.library.model;
// Import annotations Lombok untuk mengurangi boilerplate code
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

// Import Java SQL classes untuk handle timestamp
import java.sql.Timestamp;

/**
 * Entity class yang merepresentasikan tabel 'users' dalam database
 * Menggunakan Lombok annotations untuk generate getter, setter,
 * constructor otomatis
 */
@Data // Lombok: Generate getter, setter, toString, equals, hashCode otomatis
@Builder // Lombok: Implement builder pattern untuk object creation
@NoArgsConstructor // Lombok: Generate constructor tanpa parameter
@AllArgsConstructor // Lombok: Generate constructor dengan semua parameter
public class User {

    // Primary key dari tabel users
    private Integer userId;

    // Username unik untuk login
    private String username;

    // Email unik untuk komunikasi
    private String email;

    // Nama lengkap user
    private String fullName;

    // Nomor telepon (optional)
    private String phone;

    // Role user: member, librarian, admin
    private String role;

    // Status akun: active, inactive, suspended
    private String status;

    // Tanggal pendaftaran user (auto-generated)
    private Timestamp registrationDate;

    // Tanggal login terakhir (optional)
    private Timestamp lastLogin;

    // Timestamp ketika record dibuat (auto-generated)
    private Timestamp createdAt;

    // Timestamp ketika record terakhir diupdate (auto-generated oleh trigger)
    private Timestamp updatedAt;
}