package io.sustc.service.impl;

import io.sustc.dto.ReviewRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.RecipeRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

    @Override
    public List<Integer> getGroupMembers() {
        return Arrays.asList(12210824, 12210823);
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    @Transactional
    public void importData(
            List<ReviewRecord> reviewRecords,
            List<UserRecord> userRecords,
            List<RecipeRecord> recipeRecords) {

        // ddl to create tables.
        createTables();

        // TODO: implement your import logic
        insertUsers(userRecords);

        insertRecipes(recipeRecords);

        insertReviews(reviewRecords);


    }


    private void createTables() {
        String[] createTableSQLs = {
                // 创建users表
                "CREATE TABLE IF NOT EXISTS users (" +
                        "    AuthorId BIGINT PRIMARY KEY, " +
                        "    AuthorName VARCHAR(255) NOT NULL, " +
                        "    Gender VARCHAR(10) CHECK (Gender IN ('Male', 'Female')), " +
                        "    Age INTEGER CHECK (Age > 0), " +
                        "    Followers INTEGER DEFAULT 0 CHECK (Followers >= 0), " +
                        "    Following INTEGER DEFAULT 0 CHECK (Following >= 0), " +
                        "    Password VARCHAR(255), " +
                        "    IsDeleted BOOLEAN DEFAULT FALSE" +
                        ")",

                // 创建recipes表
                "CREATE TABLE IF NOT EXISTS recipes (" +
                        "    RecipeId BIGINT PRIMARY KEY, " +
                        "    Name VARCHAR(500) NOT NULL, " +
                        "    AuthorId BIGINT NOT NULL, " +
                        "    CookTime VARCHAR(50), " +
                        "    PrepTime VARCHAR(50), " +
                        "    TotalTime VARCHAR(50), " +
                        "    DatePublished TIMESTAMP, " +
                        "    Description TEXT, " +
                        "    RecipeCategory VARCHAR(255), " +
                        "    AggregatedRating DECIMAL(3,2) CHECK (AggregatedRating >= 0 AND AggregatedRating <= 5), " +
                        "    ReviewCount INTEGER DEFAULT 0 CHECK (ReviewCount >= 0), " +
                        "    Calories DECIMAL(10,2), " +
                        "    FatContent DECIMAL(10,2), " +
                        "    SaturatedFatContent DECIMAL(10,2), " +
                        "    CholesterolContent DECIMAL(10,2), " +
                        "    SodiumContent DECIMAL(10,2), " +
                        "    CarbohydrateContent DECIMAL(10,2), " +
                        "    FiberContent DECIMAL(10,2), " +
                        "    SugarContent DECIMAL(10,2), " +
                        "    ProteinContent DECIMAL(10,2), " +
                        "    RecipeServings VARCHAR(100), " +
                        "    RecipeYield VARCHAR(100), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                // 创建reviews表
                "CREATE TABLE IF NOT EXISTS reviews (" +
                        "    ReviewId BIGINT PRIMARY KEY, " +
                        "    RecipeId BIGINT NOT NULL, " +
                        "    AuthorId BIGINT NOT NULL, " +
                        "    Rating INTEGER, " +
                        "    Review TEXT, " +
                        "    DateSubmitted TIMESTAMP, " +
                        "    DateModified TIMESTAMP, " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                // 创建recipe_ingredients表
                "CREATE TABLE IF NOT EXISTS recipe_ingredients (" +
                        "    RecipeId BIGINT, " +
                        "    IngredientPart VARCHAR(500), " +
                        "    PRIMARY KEY (RecipeId, IngredientPart), " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId)" +
                        ")",

                // 创建review_likes表
                "CREATE TABLE IF NOT EXISTS review_likes (" +
                        "    ReviewId BIGINT, " +
                        "    AuthorId BIGINT, " +
                        "    PRIMARY KEY (ReviewId, AuthorId), " +
                        "    FOREIGN KEY (ReviewId) REFERENCES reviews(ReviewId), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                // 创建user_follows表
                "CREATE TABLE IF NOT EXISTS user_follows (" +
                        "    FollowerId BIGINT, " +
                        "    FollowingId BIGINT, " +
                        "    PRIMARY KEY (FollowerId, FollowingId), " +
                        "    FOREIGN KEY (FollowerId) REFERENCES users(AuthorId), " +
                        "    FOREIGN KEY (FollowingId) REFERENCES users(AuthorId), " +
                        "    CHECK (FollowerId != FollowingId)" +
                        ")"
        };

        for (String sql : createTableSQLs) {
            jdbcTemplate.execute(sql);
        }
    }

    private void insertUsers(List<UserRecord> users) {
        String sql = """
        INSERT INTO users
        (AuthorId, AuthorName, Gender, Age, Followers, Following, Password, IsDeleted)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (AuthorId) DO NOTHING
        """;

        int batchSize = 1000;

        for (int i = 0; i < users.size(); i += batchSize) {
            List<UserRecord> sub =
                    users.subList(i, Math.min(i + batchSize, users.size()));

            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    UserRecord u = sub.get(i);
                    ps.setLong(1, u.getAuthorId());
                    ps.setString(2, u.getAuthorName());
                    ps.setString(3, u.getGender());
                    ps.setInt(4, u.getAge());
                    ps.setInt(5, u.getFollowers());
                    ps.setInt(6, u.getFollowing());
                    ps.setString(7, u.getPassword());
                    ps.setBoolean(8, u.isDeleted());
                }

                @Override
                public int getBatchSize() {
                    return sub.size();
                }
            });
        }
    }

    private void insertRecipes(List<RecipeRecord> recipes) {
        String sql = """
        INSERT INTO recipes
        (RecipeId, Name, AuthorId, CookTime, PrepTime, TotalTime,
         DatePublished, Description, RecipeCategory,
         AggregatedRating, ReviewCount,
         Calories, FatContent, SaturatedFatContent,
         CholesterolContent, SodiumContent,
         CarbohydrateContent, FiberContent, SugarContent,
         ProteinContent, RecipeServings, RecipeYield)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (RecipeId) DO NOTHING
        """;

        int batchSize = 1000;

        for (int i = 0; i < recipes.size(); i += batchSize) {
            List<RecipeRecord> sub =
                    recipes.subList(i, Math.min(i + batchSize, recipes.size()));

            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    RecipeRecord r = sub.get(i);
                    ps.setLong(1, r.getRecipeId());
                    ps.setString(2, r.getName());
                    ps.setLong(3, r.getAuthorId());
                    ps.setString(4, r.getCookTime());
                    ps.setString(5, r.getPrepTime());
                    ps.setString(6, r.getTotalTime());
                    ps.setTimestamp(7, r.getDatePublished());
                    ps.setString(8, r.getDescription());
                    ps.setString(9, r.getRecipeCategory());
                    ps.setFloat(10, r.getAggregatedRating());
                    ps.setInt(11, r.getReviewCount());
                    ps.setFloat(12, r.getCalories());
                    ps.setFloat(13, r.getFatContent());
                    ps.setFloat(14, r.getSaturatedFatContent());
                    ps.setFloat(15, r.getCholesterolContent());
                    ps.setFloat(16, r.getSodiumContent());
                    ps.setFloat(17, r.getCarbohydrateContent());
                    ps.setFloat(18, r.getFiberContent());
                    ps.setFloat(19, r.getSugarContent());
                    ps.setFloat(20, r.getProteinContent());
                    ps.setInt(21, r.getRecipeServings());
                    ps.setString(22, r.getRecipeYield());
                }

                @Override
                public int getBatchSize() {
                    return sub.size();
                }
            });
        }
    }

    private void insertReviews(List<ReviewRecord> reviews) {
        String sql = """
        INSERT INTO reviews
        (ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (ReviewId) DO NOTHING
        """;

        int batchSize = 1000;

        for (int i = 0; i < reviews.size(); i += batchSize) {
            List<ReviewRecord> sub =
                    reviews.subList(i, Math.min(i + batchSize, reviews.size()));

            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    ReviewRecord r = sub.get(i);
                    ps.setLong(1, r.getReviewId());
                    ps.setLong(2, r.getRecipeId());
                    ps.setLong(3, r.getAuthorId());
                    ps.setFloat(4, r.getRating());
                    ps.setString(5, r.getReview());
                    ps.setTimestamp(6, r.getDateSubmitted());
                    ps.setTimestamp(7, r.getDateModified());
                }

                @Override
                public int getBatchSize() {
                    return sub.size();
                }
            });
        }
    }

    private void insertRecipeIngredients(List<RecipeRecord> recipes) {
        String sql = "INSERT INTO recipe_ingredients (RecipeId, IngredientPart) VALUES (?, ?) ON CONFLICT DO NOTHING";

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                RecipeRecord r = recipes.get(i);
                ps.setLong(1, r.getRecipeId());
                ps.setString(2, r.ingredientPart());
            }

            @Override
            public int getBatchSize() { return recipes.size(); }
        });
    }


    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void drop() {
        // You can use the default drop script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.
        // This method will delete all the tables in the public schema.

        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'DROP TABLE IF EXISTS ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
