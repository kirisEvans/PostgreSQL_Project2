package io.sustc.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FeedItem implements Serializable {

    /**
     * Unique identifier of the recipe.
     */
    private long recipeId;

    /**
     * Title or display name of the recipe.
     */
    private String name;

    /**
     * ID of the author who created this recipe.
     */
    private long authorId;

    /**
     * Name of the author who created this recipe.
     */
    private String authorName;

    /**
     * Timestamp indicating when the recipe was published.
     */
    private Instant datePublished;

    /**
     * Aggregated rating value computed from all reviews of this recipe.
     */
    private Double aggregatedRating;

    /**
     * Total number of reviews associated with this recipe.
     */
    private Integer reviewCount;
}