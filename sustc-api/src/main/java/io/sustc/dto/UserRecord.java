package io.sustc.dto;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The user record used for data import
 * @implNote You may implement your own {@link java.lang.Object#toString()} since the default one in {@link lombok.Data} prints all array values.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserRecord implements Serializable {

    /**
     * The id of this author, unique
     */
    private long authorId;

    /**
     * The name of this author
     */
    private String authorName;

    /**
     * The gender of this author. Gender in {Male, Female}
     */
    private String gender;

    /**
     * The age of this author
     */
    private int age;

    /**
     * The number of users who follow this author
     */
    private int followers;

    /**
     * The number of users followed by this author
     */
    private int following;

    /**
     * List of users who follow this author
     */
    private long[] followerUsers;

    /**
     * List of users followed by this author
     */
    private long[] followingUsers;

    private String password;

    /**
     * Indicates whether the user has been deleted (soft delete).
     */
    private boolean isDeleted;

}
