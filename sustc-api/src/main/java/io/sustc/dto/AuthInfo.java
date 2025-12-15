package io.sustc.dto;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Authentication and identity information used across the system.
 *
 * <p>This class carries the identity of the current user. It may contain a
 * password for login-related operations, but <b>password validation is ONLY
 * required inside {@code UserService}</b> (e.g., register, login, deleteAccount,
 * updateProfile). Other services such as {@code RecipeService},
 * {@code ReviewService}, {@code SocialService}, or {@code FeedService}
 * <b>must NOT perform password checks</b>; they only verify that:
 * <ul>
 *   <li>{@code authorId} corresponds to an existing user</li>
 *   <li>the user is active (not soft-deleted)</li>
 * </ul>
 *
 * <p>Password storage and hashing mechanisms are intentionally left for
 * students to design. The provided dataset does not include passwords, so
 * implementations may choose to:
 * <ul>
 *   <li>store passwords upon registration, and validate them in login;</li>
 *   <li>or use alternative authentication logic during development/testing.</li>
 * </ul>
 *
 * <p>In summary:
 * <ul>
 *   <li><b>UserService:</b> must validate passwords where appropriate</li>
 *   <li><b>All other services:</b> must NOT validate passwords, only userId + active</li>
 * </ul>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuthInfo implements Serializable {

    /**
     * The user's id.
     */
    private long authorId;

    /**
     * The password used when login by id.
     */
    private String password;

}
