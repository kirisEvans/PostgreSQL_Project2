package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.dto.ReviewRecord;
import io.sustc.service.RecipeService;
import io.sustc.service.ReviewService;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
public class ReviewServiceImpl implements ReviewService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    @Transactional
    public long addReview(AuthInfo auth, long recipeId, int rating, String review) {
        String sql1 = "select IsDeleted from users where " +
                "authorId = (SELECT authorId FROM recipes WHERE RecipeId = ?)";
        String sql2 = "select password from users where AuthorId = ?";

        if (rating > 5 || rating < 1 || auth == null) {
            throw new IllegalArgumentException("rating or auth wrong");
        }

        try {
            if (jdbcTemplate.queryForObject(sql1, Boolean.class, recipeId)) {
                throw new IllegalArgumentException("user does not exist");
            }

            String password = jdbcTemplate.queryForObject(sql2, String.class, auth.getAuthorId());
            if (!password.equals(auth.getPassword())) {
                throw new IllegalArgumentException("password wrong");
            }

        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Recipe does not exist");
        }

        String sql3 = "select max(reviewId) from reviews";
        Long max_id = jdbcTemplate.queryForObject(sql3, Long.class);
        if (max_id == null) {
            max_id = 1L;
        }
        max_id++;

        String sql4 = """
        INSERT INTO reviews
        (ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (ReviewId) DO NOTHING
        """;

        jdbcTemplate.update(sql4,
                max_id,
                recipeId,
                auth.getAuthorId(),
                rating,
                review
        );

        String sql5 = "update recipes set AggregatedRating = " +
                "round((select avg(Rating) from reviews " +
                "where RecipeId = ?), 3), ReviewCount = ReviewCount + 1 where RecipeId = ?;";

        jdbcTemplate.update(sql5, recipeId, recipeId);
        return max_id;
    }

    @Override
    @Transactional
    public void editReview(AuthInfo auth, long recipeId, long reviewId, int rating, String review) {
        String sql = "select recipeId from recipes where recipeId = ?";
        String sql1 = "select authorId from reviews where reviewId = ?";
        String sql2 = "select password, isDeleted from users where AuthorId = ?";

        if (rating > 5 || rating < 1 || auth == null) {
            throw new IllegalArgumentException("rating or auth wrong");
        }

        try {
            jdbcTemplate.queryForObject(sql, Long.class, recipeId);

            long authorId = jdbcTemplate.queryForObject(sql1, Long.class, reviewId);

            if (authorId != auth.getAuthorId()) {
                throw new SecurityException("author doesn't match");
            }

             jdbcTemplate.queryForObject(sql2, (rs, rowNum) -> {
                String password = rs.getString("password");
                boolean isDeleted = rs.getBoolean("isDeleted");

                if (!password.equals(auth.getPassword()) || isDeleted) {
                     throw new IllegalArgumentException("password wrong or user is deleted");
                }
                return null;
            }, auth.getAuthorId());
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Recipe does not exist");
        }

        String sql3 = "update reviews set rating = ?, review = ? where ReviewId = ?;";
        jdbcTemplate.update(sql3, rating, review, reviewId);

        String sql4 = "update recipes set AggregatedRating = " +
                "round((select avg(Rating) from reviews " +
                "where RecipeId = ?), 3);";
        jdbcTemplate.update(sql4, recipeId);
    }

    @Override
    @Transactional
    public void deleteReview(AuthInfo auth, long recipeId, long reviewId) {
        return ;
    }

    @Override
    @Transactional
    public long likeReview(AuthInfo auth, long reviewId) {
        return 0;
    }

    @Override
    @Transactional
    public long unlikeReview(AuthInfo auth, long reviewId) {
        return 0;
    }

    @Override
    public PageResult<ReviewRecord> listByRecipe(long recipeId, int page, int size, String sort) {
        return null;
    }

    @Override
    @Transactional
    public RecipeRecord refreshRecipeAggregatedRating(long recipeId) {
        return null;
    }

}