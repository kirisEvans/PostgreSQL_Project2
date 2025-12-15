package io.sustc.service;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import org.springframework.lang.Nullable;

import java.util.List;
import java.util.Map;


public interface RecipeService {

    /**
     * get the name of recipe with id.
     * This method only used as test.
     *
     * @param id the recipe id
     * @return the name of this recipe
     */
    String getNameFromID(long id);

    /**
     * Retrieves a recipe by its ID.
     *
     * <p>This method returns the <b>complete {@link RecipeRecord}</b> corresponding
     * to the given ID, including <b>all fields defined in {@code RecipeRecord}</b>.
     *
     * <p>This method returns {@code null} if no active (non-deleted) recipe
     * with the given ID exists.</p>
     *
     * @param recipeId the ID of the recipe to retrieve
     * @return the corresponding {@link RecipeRecord}, or {@code null} if not found
     * @throws IllegalArgumentException if {@code recipeId <= 0}
     */
    RecipeRecord getRecipeById(long recipeId);


    /**
     * Searches recipes based on multiple optional criteria, supporting pagination and sorting.
     *
     * <p><b>Filtering rules:</b>
     * <ul>
     *   <li><b>keyword</b>: performs a <em>case-insensitive fuzzy match</em> on both
     *       {@code name} and {@code description}</li>
     *   <li><b>category</b>: exact match on the {@code recipe_category} field</li>
     *   <li><b>minRating</b>: filters recipes where {@code aggregated_rating ≥ minRating}</li>
     * </ul>
     *
     * <p>Pagination:
     * <ul>
     *   <li>{@code page}: page number, starting from 1</li>
     *   <li>{@code size}: number of items per page</li>
     * </ul>
     *
     * <p>Sorting (optional):
     * <ul>
     *   <li>{@code "rating_desc"} — highest rating first</li>
     *   <li>{@code "date_desc"} — newest first</li>
     *   <li>{@code "calories_asc"} — lowest calories first</li>
     * </ul>
     *
     * <p>This method returns the <b>complete {@link RecipeRecord}</b> corresponding
     * to the given ID, including <b>all fields defined in {@code RecipeRecord}</b>.
     *
     * @param keyword   fuzzy search term for name/description (nullable)
     * @param category  category filter (nullable)
     * @param minRating minimum rating filter (nullable)
     * @param page      page number (1-based)
     * @param size      page size
     * @param sort      sorting criteria (nullable)
     * @return a {@link PageResult} containing paginated recipe results
     * @throws IllegalArgumentException if {@code page < 1} or {@code size <= 0}
     */
    PageResult<RecipeRecord> searchRecipes(
            String keyword,
            String category,
            Double minRating,
            Integer page,
            Integer size,
            String sort
    );

    /**
     * Creates a new recipe authored by the authenticated user.
     *
     * <p>Requirements:
     * <ul>
     *   <li>{@code auth} must represent a valid and active user</li>
     *   <li>Recipe name must not be null or empty</li>
     *   <li>Nutritional fields may be null</li>
     * </ul>
     *
     * @param dto  recipe data transfer object containing recipe details
     * @param auth authentication identity of the recipe creator
     * @return the newly created recipe's ID
     *
     * @throws SecurityException if the user identity in {@code auth} is invalid or inactive
     * @throws IllegalArgumentException if the recipe name is null/empty, or required fields are invalid
     */
    long createRecipe(RecipeRecord dto, AuthInfo auth);


    /**
     *
     * Permanently deletes a recipe by its ID.
     *
     * <p>Only the recipe author may delete the recipe.</p>
     *
     * <p><b>Requirements:</b></p>
     * <ul>
     *   <li>The recipe record is physically removed from the database.</li>
     *   <li>All data that directly depends on the recipe
     *       (including reviews)
     *       must be deleted or cascaded accordingly.</li>
     *   <li>After deletion, the recipe ID must no longer be queryable.</li>
     * </ul>
     *
     * @param recipeId the ID of the recipe
     * @param auth     authentication identity of the operator
     *
     * @throws SecurityException if {@code auth} is invalid, inactive,
     *         or if the operator is not the recipe author
     */
    void deleteRecipe(long recipeId, AuthInfo auth);


    /**
     * Updates recipe preparation time and cooking time, and automatically recalculates total time.
     *
     * <p><b>Authorization Rules:</b></p>
     * <ul>
     *   <li>Only the recipe author is allowed to update recipe times.</li>
     *   <li>If the authenticated user is not the recipe owner, a {@link SecurityException} must be thrown.</li>
     *   <li>Deleted / inactive users cannot perform this operation.</li>
     * </ul>
     *
     * <p><b>Specifications:</b></p>
     * <ul>
     *   <li>cookTimeIso / prepTimeIso must be valid ISO 8601 duration strings, e.g. "PT30M", "PT1H30M", "P1DT5H".</li>
     *   <li>If a parameter is {@code null}, the corresponding field is not modified.</li>
     *   <li>totalTime is always computed as cookTime + prepTime; clients cannot directly set totalTime.</li>
     *   <li>If either input string is invalid, an {@link IllegalArgumentException} must be thrown and no partial updates are written.</li>
     *   <li>Negative durations or overflow after parsing must be treated as invalid requests.</li>
     * </ul>
     *
     * <p><b>Implementation Suggestions:</b></p>
     * <ul>
     *   <li>Use {@code java.time.Duration.parse(...)} to parse ISO 8601 durations.</li>
     *   <li>Store parsed values in integer *_sec fields internally.</li>
     *   <li>Store the original string in *_iso fields for display purposes.</li>
     * </ul>
     *
     * @param auth        authentication information of the operator
     * @param recipeId    the recipe being updated
     * @param cookTimeIso new cooking time (ISO 8601), or null to keep unchanged
     * @param prepTimeIso new preparation time (ISO 8601), or null to keep unchanged
     * @throws SecurityException if {@code auth} is invalid, inactive, or if the authenticated user is not the recipe author
     * @throws IllegalArgumentException if the provided ISO 8601 strings are invalid
     */
    void updateTimes(AuthInfo auth, long recipeId, @Nullable String cookTimeIso, @Nullable String prepTimeIso);


    /**
     * Finds the pair of recipes whose calorie values are closest to each other.
     *
     * <p><b>Task Definition:</b></p>
     * Among all recipes with a non-null {@code Calories} field, find the pair
     * {@code (RecipeA, RecipeB)} such that the absolute calorie difference:
     *
     * <pre>
     *     |CaloriesA - CaloriesB|
     * </pre>
     *
     * is minimized.
     *
     * <p><b>Output Format:</b></p>
     * This method returns a {@code Map<String, Object>} containing:
     *
     * <ul>
     *     <li>{@code "RecipeA"} – {@code Long}, the smaller recipe ID in the pair</li>
     *     <li>{@code "RecipeB"} – {@code Long}, the larger recipe ID in the pair</li>
     *     <li>{@code "CaloriesA"} – {@code Double}, the calories of RecipeA</li>
     *     <li>{@code "CaloriesB"} – {@code Double}, the calories of RecipeB</li>
     *     <li>{@code "Difference"} – {@code Double}, the absolute difference between the two</li>
     * </ul>
     *
     * <p><b>Tie-breaking Rules:</b></p>
     * If multiple pairs have the same minimal difference:
     *
     * <ol>
     *     <li>Select the pair with the smaller {@code RecipeA}.</li>
     *     <li>If still tied, select the pair with the smaller {@code RecipeB}.</li>
     * </ol>
     *
     * <p><b>Corner Cases:</b></p>
     * <ul>
     *     <li>If fewer than two recipes contain non-null {@code Calories},
     *         this method must return {@code null}.</li>
     *     <li>All comparisons must be computed using SQL (no in-memory full-table loading).</li>
     * </ul>
     *
     * @return a map describing the closest-calorie recipe pair, or {@code null} if no eligible pair exists.
     */
    Map<String, Object> getClosestCaloriePair();


    /**
     * Retrieves the top 3 recipes with the greatest number of ingredient entries
     * recorded in {@code recipe_ingredients}.
     *
     * <p><b>Task Definition:</b></p>
     * Each row in {@code recipe_ingredients} represents one ingredient part
     * associated with a recipe. The total number of rows for a given
     * {@code RecipeId} therefore reflects how many ingredient parts are required
     * for that recipe.
     *
     * <p><b>Counting Rules:</b></p>
     * <ul>
     *     <li>Only recipes that appear in {@code recipe_ingredients} should be included.</li>
     *     <li>The number of ingredients for a recipe is computed as:
     *         {@code COUNT(IngredientPart)} grouped by {@code RecipeId}.</li>
     *     <li>Each unique {@code (RecipeId, IngredientPart)} pair counts as exactly one ingredient.</li>
     * </ul>
     *
     * <p><b>Returned Fields:</b></p>
     * Each result row is provided as a {@code Map<String, Object>} containing:
     * <ul>
     *     <li>{@code "RecipeId"} – {@code Long}, the recipe ID</li>
     *     <li>{@code "Name"} – {@code String}, the recipe name</li>
     *     <li>{@code "IngredientCount"} – {@code Integer}, the total number of ingredients</li>
     * </ul>
     *
     * <p><b>Ordering Rules:</b></p>
     * <ol>
     *     <li>Sort by {@code IngredientCount} in descending order.</li>
     *     <li>If tied, sort by {@code RecipeId} in ascending order
     *         to ensure deterministic output.</li>
     * </ol>
     *
     * <p><b>Output Size:</b></p>
     * <ul>
     *     <li>Return at most 3 recipes.</li>
     *     <li>If fewer than 3 recipes have ingredient records,
     *         return only the available entries.</li>
     * </ul>
     *
     * <p><b>Corner Cases:</b></p>
     * <ul>
     *     <li>Recipes with no ingredient entries in
     *         {@code recipe_ingredients} must be excluded.</li>
     *     <li>All returned map keys and their value types must match
     *         the specification to allow automated testing.</li>
     * </ul>
     *
     * @return a list of up to three maps describing the most
     *         ingredient-heavy recipes.
     */
    List<Map<String, Object>> getTop3MostComplexRecipesByIngredients();

}
