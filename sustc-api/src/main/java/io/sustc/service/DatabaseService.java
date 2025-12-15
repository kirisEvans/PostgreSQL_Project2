package io.sustc.service;

import io.sustc.dto.ReviewRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.RecipeRecord;

import java.util.List;

public interface DatabaseService {

    /**
     * Acknowledges the authors of this project.
     *
     * @return a list of group members' student-id
     */
    List<Integer> getGroupMembers();

    /**
     * Imports data to an empty database.
     * Invalid data will not be provided.
     *
     * @param reviewRecords review records parsed from csv
     * @param userRecords  user records parsed from csv
     * @param recipeRecords recipe records parsed from csv
     */
    void importData(
            List<ReviewRecord> reviewRecords,
            List<UserRecord> userRecords,
            List<RecipeRecord> recipeRecords
    );

    /**
     * Delete all tables in the database.
     * <p>
     * This would only be used in local benchmarking to help you
     * clean the database, and won't affect your score.
     *
     */
    void drop();

    /**
     * Sums up two numbers via Postgres.
     * This method only demonstrates how to access database via JDBC.
     *
     * @param a the first number
     * @param b the second number
     * @return the sum of two numbers
     */
    Integer sum(int a, int b);
}
