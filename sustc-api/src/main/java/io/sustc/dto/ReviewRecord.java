package io.sustc.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * The review record used for data import
 * @implNote You may implement your own {@link java.lang.Object#toString()} since the default one in {@link lombok.Data} prints all array values.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReviewRecord implements Serializable {

    /**
     * The id of this review, unique
     */
    private long reviewId;

    /**
     * The id of the reviewed recipe
     */
    private long recipeId;

    /**
     * The id of this review's author
     */
    private long authorId;

    /**
     * The name of this review's author
     */
    private String authorName;

    /**
     * The score given to this recipe
     */
    private float rating;

    /**
     * Review content
     */
    private String review;

    /**
     * The date of review submitted
     */
    private Timestamp dateSubmitted;

    /**
     * The date of review modified
     */
    private Timestamp dateModified;

    /**
     * List of users who have given this review a like
     */
    private long[] likes;
}
