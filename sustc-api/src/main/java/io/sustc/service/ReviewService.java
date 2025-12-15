package io.sustc.service;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.dto.ReviewRecord;


public interface ReviewService {

    /**
     * Adds a new review for a recipe and refreshes the recipe's
     * {@code aggregated_rating} and {@code review_count} fields.
     *
     * <p><b>Requirements:</b></p>
     * <ul>
     *   <li>The target {@code recipeId} must exist and must refer to a non-deleted recipe.</li>
     *   <li>{@code auth} must refer to a valid and active user.</li>
     *   <li>The authenticated user (from {@code auth}) becomes the review author.</li>
     *   <li>{@code rating} must be within the inclusive range {@code [1, 5]}.</li>
     * </ul>
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     *   <li>After insertion, the recipe’s aggregated_rating must be recalculated using
     *       the average of all non-deleted reviews, rounded to two decimal places.</li>
     *   <li>If the recipe has no remaining reviews after this operation, its
     *       aggregated_rating becomes {@code null} and review_count becomes {@code 0}.</li>
     * </ul>
     *
     * <p><b>Exception Rules:</b></p>
     * <ul>
     *   <li>{@link IllegalArgumentException} is thrown if:
     *     <ul>
     *       <li>{@code recipeId} does not exist</li>
     *       <li>{@code rating} is outside the range [1, 5]</li>
     *       <li>{@code auth} is null or fails validation</li>
     *     </ul>
     *   </li>
     *
     *   <li>{@link SecurityException} is thrown if the authenticated user is inactive (soft-deleted)</li>

     * </ul>
     *
     * @param auth     authentication information of the user writing the review
     * @param recipeId the ID of the recipe being reviewed
     * @param rating   review rating between 1 and 5
     * @param review   textual review content
     * @return the newly created review ID
     *
     * @throws SecurityException if the user identity in {@code auth} is invalid or inactive
     */
    long addReview(AuthInfo auth, long recipeId, int rating, String review);

    /**
     * Edits an existing review belonging to a given recipe.
     *
     * <p><b>Authorization Rules:</b></p>
     * <ul>
     *   <li>Only the original author of the review may edit it.</li>
     *   <li>Deleted / inactive users cannot perform this operation.</li>
     * </ul>
     *
     * <p><b>Consistency Rules:</b></p>
     * <ul>
     *   <li>The specified {@code reviewId} must belong to the given {@code recipeId};
     *       otherwise an {@link IllegalArgumentException} must be thrown.</li>
     *   <li>{@code rating} must be within the range [1, 5].</li>
     * </ul>
     *
     * <p><b>Post-conditions:</b></p>
     * <ul>
     *   <li>The review text and rating are updated.</li>
     *   <li>The recipe’s aggregated_rating and review_count must be refreshed afterward.</li>
     * </ul>
     *
     * @param auth     authentication information of the operator
     * @param recipeId the recipe to which the review belongs
     * @param reviewId the ID of the review to edit
     * @param rating   updated rating in the range [1, 5]
     * @param review   updated textual review content
     * @throws SecurityException        if {@code auth} is invalid, inactive, or if the operator is not the author of the review
     * @throws IllegalArgumentException if the review does not belong to the recipe
     * @throws IllegalArgumentException if the rating is outside the range [1, 5]
     */
    void editReview(AuthInfo auth, long recipeId, long reviewId, int rating, String review);


    /**
     * Permanently deletes an existing review belonging to a given recipe, along with its likes.
     *
     * <p><b>Authorization Rules:</b></p>
     * <ul>
     *   <li>Only the review author (or an administrator if designed) may delete the review.</li>
     *   <li>Deleted / inactive users cannot perform this operation.</li>
     * </ul>
     *
     * <p><b>Consistency Rules:</b></p>
     * <ul>
     *   <li>The specified {@code reviewId} must belong to the given {@code recipeId};
     *       otherwise an {@link IllegalArgumentException} must be thrown.</li>
     * </ul>
     *
     * <p><b>Post-conditions (Hard Delete):</b></p>
     * <ul>
     *   <li>The review entry is permanently removed from the database.</li>
     *   <li>All likes associated with the review are permanently removed.</li>
     *   <li>The recipe’s aggregated_rating and review_count must be recalculated afterward.</li>
     * </ul>
     *
     * @param auth     authentication identity of the operator
     * @param recipeId the ID of the recipe to which the review belongs
     * @param reviewId the ID of the review to delete
     *
     * @throws SecurityException        if {@code auth} is invalid, inactive,
     *                                  or if the operator is not allowed to delete the review
     * @throws IllegalArgumentException if the review does not belong to the recipe
     */
    void deleteReview(AuthInfo auth, long recipeId, long reviewId);


    /**
     * Adds a like to the specified review.
     *
     * <p><b>Constraints:</b></p>
     * <ul>
     *   <li>The operator (given by {@code auth}) must be a valid, active user.</li>
     *   <li>The target review must exist.</li>
     *   <li>Users cannot like their own reviews.</li>
     *   <li>The pair {@code (userId, reviewId)} must be unique; repeated likes from the same user should do nothing.</li>
     * </ul>
     *
     * @param auth     authentication identity of the user performing the like
     * @param reviewId the ID of the review to like
     * @return the total number of likes currently associated with the review
     *
     * @throws SecurityException
     *         if {@code auth} is invalid, inactive, or if the user attempts to like their own review
     *
     * @throws IllegalArgumentException
     *         if the review does not exist
     */
    long likeReview(AuthInfo auth, long reviewId);

    /**
     * Removes a like from the specified review.
     *
     * <p><b>Constraints:</b></p>
     * <ul>
     *   <li>The operator (given by {@code auth}) must be a valid, active user.</li>
     *   <li>The target review must exist.</li>
     *   <li>If the user never liked the review, the operation becomes a no-op.</li>
     * </ul>
     *
     * @param auth     authentication identity of the user performing the unlike
     * @param reviewId the ID of the review to unlike
     * @return the total number of likes currently associated with the review
     *
     * @throws SecurityException
     *         if {@code auth} is invalid or inactive
     *
     * @throws IllegalArgumentException
     *         if the review does not exist
     */
    long unlikeReview(AuthInfo auth, long reviewId);


    /**
     * Lists reviews for a specific recipe with pagination and sorting.
     *
     * <p>deleted reviews must not appear.
     *
     * <p>Supported sort options:
     * <ul>
     *     <li><code>date_desc</code> — newest reviews first, based on {@code DateModified}</li>
     *     <li><code>likes_desc</code> — reviews with the highest number of likes first</li>
     * </ul>
     *
     * <p>Pagination rules:
     * <ul>
     *     <li>{@code page} is 1-based</li>
     *     <li>{@code size} must be positive</li>
     * </ul>
     *
     * @param recipeId the ID of the recipe whose reviews are being listed
     * @param page     the page index (starting from 1)
     * @param size     the number of items per page
     * @param sort     sorting option, such as <code>"date_desc"</code> or <code>"likes_desc"</code>
     * @return a paginated {@link PageResult} containing {@link ReviewRecord} entries
     * @throws IllegalArgumentException if {@code page < 1} or {@code size <= 0}
     */
    PageResult<ReviewRecord> listByRecipe(long recipeId, int page, int size, String sort);

    /**
     * Recalculates and updates the {@code aggregated_rating} and {@code review_count}
     * fields for the specified recipe.
     *
     * <p>This method must ensure:</p>
     * <ul>
     *     <li>{@code aggregated_rating} rounded to two decimal places.</li>
     *     <li>{@code review_count} of all existing reviews belonging to the recipe.</li>
     *     <li>If the recipe has no remaining reviews:
     *         <ul>
     *             <li>{@code aggregated_rating} must be set to {@code null};</li>
     *             <li>{@code review_count} must be set to {@code 0}.</li>
     *         </ul>
     *     </li>
     * </ul>
     *
     * <p>Additional constraints:</p>
     * <ul>
     *     <li>Deleted reviews must be ignored when computing statistics.</li>
     *     <li>The update must be atomic — no partial writes are allowed if any failure occurs.</li>
     * </ul>
     *
     * @param recipeId the ID of the recipe whose rating statistics should be recomputed
     * @return the updated {@link RecipeRecord} reflecting the recalculated rating and review count
     * @throws IllegalArgumentException if {@code recipeId} does not exist or refers to a deleted recipe
     */
    RecipeRecord refreshRecipeAggregatedRating(long recipeId);

}
