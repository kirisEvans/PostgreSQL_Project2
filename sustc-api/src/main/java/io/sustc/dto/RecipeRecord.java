package io.sustc.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * The recipe record used for data import
 *
 * @implNote You may implement your own {@link java.lang.Object#toString()} since the default one in {@link lombok.Data} prints all array values.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RecipeRecord implements Serializable {

    /**
     * The id of recipe, unique
     */
    private long RecipeId;

    /**
     * The name of recipe
     */
    private String name;

    /**
     * The id of this recipe's author
     */
    private long authorId;

    /**
     * The name of this recipe's author
     */
    private String authorName;

    /**
     * Cooking operation time in ISO 8601 duration format (e.g., PT1H30M for one hour thirty minutes)
     */
    private String cookTime;

    /**
     * Preparation time in ISO 8601 duration format
     */
    private String prepTime;

    /**
     * Total time (CookTime + PrepTime) in ISO 8601 duration format
     */
    private String totalTime;

    /**
     * The release time of this recipe
     */
    private Timestamp datePublished;

    /**
     * Description created by author
     */
    private String description;

    /**
     * The category of this recipe belong to
     */
    private String recipeCategory;

    /**
     * Ingredients composition of this recipe
     */
    private String[] recipeIngredientParts;

    /**
     * The score obtained of this recipe
     */
    private float aggregatedRating;

    /**
     * The reviewer's number of this recipe
     */
    private int reviewCount;

    /**
     * Calories of this recipe
     */
    private float calories;

    /**
     * Fat content
     */
    private float fatContent;

    /**
     * Saturated fat content
     */
    private float saturatedFatContent;

    /**
     * Cholesterol content
     */
    private float cholesterolContent;

    /**
     * Sodium content
     */
    private float sodiumContent;

    /**
     * Carbohydrate content
     */
    private float carbohydrateContent;

    /**
     * Fiber content
     */
    private float fiberContent;

    /**
     * Sugar content
     */
    private float sugarContent;

    /**
     * Protein content
     */
    private float proteinContent;

    /**
     * The number of people that the recipe can serve
     */
    private int recipeServings;

    /**
     * The output of the recipe (the quantity, weight or volume of the food)
     */
    private String recipeYield;

}