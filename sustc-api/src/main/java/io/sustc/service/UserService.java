package io.sustc.service;

import io.sustc.dto.*;
import org.springframework.lang.Nullable;

import java.util.List;
import java.util.Map;

public interface UserService {

    /**
     * Registers a new user in the system.
     *
     * <p>The following fields in {@code req} are required:
     * <ul>
     *   <li>{@code authorName}</li>
     *   <li>{@code gender}</li>
     *   <li>{@code age}</li>
     * </ul>
     *
     *
     * @param req the registration request containing user's basic profile
     * @return the new user's {@code authorId}; return {@code -1} if registration fails
     *
     * @apiNote Consider the following corner cases:
     * <ul>
     *   <li>{@code authorName} is null or empty</li>
     *   <li>{@code gender} is null, empty, or not in {"Male", "Female"}</li>
     *   <li>{@code age} is null or not a valid positive integer</li>
     *   <li>another user with the same {@code authorName} already exists</li>
     * </ul>
     *
     * If any corner case occurs, this method must return {@code -1}.
     */
    long register(RegisterUserReq req);

    /**
     * Authenticates a user using password-based login.
     *
     * <p>Requirements and rules:</p>
     * <ul>
     *   <li>Only password-based login is allowed.</li>
     *   <li>The {@code authorId} in {@code auth} must refer to an existing user.</li>
     *   <li>The target user must be active (not soft-deleted).</li>
     *   <li>The password in {@code auth} must match the stored password hash of the user.</li>
     * </ul>
     *
     * <p>Return value:</p>
     * <ul>
     *   <li>Return the authenticated user's {@code authorId} on success.</li>
     *   <li>Return {@code -1} if authentication fails.</li>
     * </ul>
     *
     * <p>Corner cases where authentication fails (return -1):</p>
     * <ul>
     *   <li>{@code auth} is null</li>
     *   <li>{@code auth.authorId} is invalid (user does not exist)</li>
     *   <li>user is soft-deleted (inactive)</li>
     *   <li>{@code auth.password} is null or empty</li>
     *   <li>password does not match the user's stored password hash</li>
     * </ul>
     *
     * <p>No exception should be thrown for login failures; return {@code -1} instead.</p>
     *
     * <p>Other APIs in this project do <b>not</b> require strict password validation.</p>
     *
     * @param auth authentication credentials containing {@code authorId} and {@code password}
     * @return the authenticated user ID, or {@code -1} if authentication fails
     *
     */
    long login(AuthInfo auth);


    /**
     * Soft-deletes a user account.
     *
     * <p><b>Authorization Rules:</b></p>
     * <ul>
     *   <li>A user may delete their own account.</li>
     *   <li>Soft-deleted / inactive users cannot perform this operation.</li>
     * </ul>
     *
     * <p><b>Consistency Rules:</b></p>
     * <ul>
     *   <li>The specified {@code userId} must exist and refer to an active user.</li>
     * </ul>
     *
     * <p><b>Post-conditions (Soft Delete):</b></p>
     * <ul>
     *   <li>The user is marked as inactive (e.g., {@code isDeleted = true}).</li>
     *   <li>The deleted user can no longer authenticate or perform any operations.</li>
     *   <li>Existing content created by the user (recipes, reviews, likes, etc.)
     *       must remain but treated as belonging to an inactive user.</li>
     *   <li>All follow relationships involving this user must be removed:
     *       <ul>
     *         <li>this user should no longer follow anyone (clear its following list)</li>
     *         <li>no other user should follow this user (remove this user from all followers lists)</li>
     *       </ul>
     *   </li>
     * </ul>
     *
     * @param auth   authentication identity of the operator
     * @param userId the ID of the user account to soft-delete
     *
     * @return {@code true} if the account was successfully soft-deleted;
     *         {@code false} if the account is already inactive
     *
     * @throws SecurityException if {@code auth} is invalid, inactive, or if the operator is not allowed to delete the account
     * @throws IllegalArgumentException if the target user does not exist
     */
    boolean deleteAccount(AuthInfo auth, long userId);


    /**
     * Follows or unfollows a user.
     *
     * <p>This is a toggle operation:
     * <ul>
     *   <li>If the current user is not following {@code followeeId}, then follow.</li>
     *   <li>If already following, then unfollow.</li>
     * </ul>
     *
     * @param auth        authentication info of the follower
     * @param followeeId  the user to be followed or unfollowed
     * @return {@code true} if the follow state becomes "following" or "not following" after this operation,
     *         {@code false} if the operation fails
     *
     * @apiNote Consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid</li>
     *   <li>no user exists with the given {@code followeeId}</li>
     *   <li>a user attempts to follow themselves</li>
     * </ul>
     *
     * @throws SecurityException
     *         if {@code auth} is invalid, inactive, or if the user attempts to follow himself/herself
     *
     */
    boolean follow(AuthInfo auth, long followeeId);

    /**
     * Retrieve basic profile information of a user by user ID.
     *
     * Requirements:
     * <ul>
     *   <li>The user with the given {@code userId} must exist.</li>
     *   <li>Returned information should contain at least: userId, userName, gender, age,
     *       followers count, following count.</li>
     *   <li>This method does not enforce permission checks; it is a public lookup.</li>
     * </ul>
     *
     * @param userId the ID of the target user
     * @return a {@link UserRecord} containing the user's basic information
     */
    UserRecord getById(long userId);


    /**
     * Update profile information of a user (e.g., gender, age).
     *
     * Requirements:
     * <ul>
     *   <li>Only the user themselves can modify their own profile.</li>
     *   <li>{@code gender} may be nullable, but if provided must be valid
     *       (e.g., "Male", "Female").</li>
     *   <li>{@code age} may be nullable, but if provided must be a positive integer.</li>
     *   <li>If a field is null, it should not be updated.</li>
     * </ul>
     *
     * @param auth        authentication info
     * @param gender new gender value (nullable)
     * @param age new age value (nullable)
     * @throws SecurityException if the user identity in {@code auth} is invalid or inactive
     */
    void updateProfile(AuthInfo auth, String gender, Integer age);


    /**
     * Returns the recipe timeline (Feed) of users followed by the current user.
     * <p>
     * Requirements:
     * <ul>
     *     <li>Only returns recipes published by users followed by the userId;</li>
     *     <li>Results are sorted by publication date descending (date_published DESC),
     *         and by recipe_id descending for stability when dates are identical;</li>
     *     <li>Supports pagination: page starts from 1, size range is 1~200
     *         (values outside this range are automatically adjusted to valid bounds);</li>
     *     <li>If category is not null, only returns recipes where recipe_category equals the specified value;</li>
     *     <li>If userId doesn't follow anyone, returns empty results with total = 0.</li>
     * </ul>
     *
     * @param auth        authentication info
     * @param page     Page number (starting from 1)
     * @param size     Page size (1~200)
     * @param category Optional recipe category filter, null means no filtering
     * @return Paginated timeline results
     *
     * @throws SecurityException if the user identity in {@code auth} is invalid or inactive
     */
    PageResult<FeedItem> feed(AuthInfo auth, int page, int size, @Nullable String category);


    /**
     * Finds the active (non-deleted) user with the highest ratio of followers to followings.
     *
     * <p><b>Definition of Counts:</b></p>
     * These values must be computed dynamically from {@code user_follows}:
     * <ul>
     *     <li><b>FollowerCount</b>: number of rows where {@code FollowingId = user.AuthorId}</li>
     *     <li><b>FollowingCount</b>: number of rows where {@code FollowerId = user.AuthorId}</li>
     * </ul>
     *
     * <p><b>Eligibility:</b></p>
     * <ul>
     *     <li>User must have {@code IsDeleted = FALSE}.</li>
     *     <li>{@code FollowingCount} must be greater than 0 (to avoid division by zero).</li>
     * </ul>
     *
     * <p><b>Ratio Definition:</b></p>
     * <pre>
     *     ratio = FollowerCount * 1.0 / FollowingCount
     * </pre>
     *
     * <p><b>Tie-breaking Rules:</b></p>
     * If multiple users share the same ratio:
     * <ol>
     *     <li>Select the user with the smallest {@code AuthorId}.</li>
     * </ol>
     *
     * <p><b>No Eligible Users Case:</b></p>
     * If no active user has a following count greater than zero,
     * then this method must return {@code null}.
     *
     * <p><b>Returned Fields:</b></p>
     * The returned map contains the following keys:
     * <ul>
     *     <li>{@code "AuthorId"} – {@code Long}, user ID</li>
     *     <li>{@code "AuthorName"} – {@code String}, user name</li>
     *     <li>{@code "Ratio"} – {@code Double}, follower/following ratio</li>
     * </ul>
     *
     * @return a map describing the user with the highest ratio, or {@code null} if no eligible user exists.
     */
    Map<String, Object> getUserWithHighestFollowRatio();

}
