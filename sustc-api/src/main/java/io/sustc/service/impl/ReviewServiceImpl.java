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

import java.util.ArrayList;
import java.util.List;

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
        String sql = "select recipeId from recipes where recipeId = ?";
        String sql1 = "select authorId, recipeId from reviews where reviewId = ?";
        String sql2 = "select password, isDeleted from users where AuthorId = ?";

        if (auth == null) {
            throw new IllegalArgumentException("rating or auth wrong");
        }

        try {
            jdbcTemplate.queryForObject(sql, Long.class, recipeId);

            jdbcTemplate.queryForObject(sql1, (rs, rowNum) -> {
                long authorId1 = rs.getLong("authorId");
                long recipeId1 = rs.getLong("recipeId");

                if (authorId1 != auth.getAuthorId()) {
                    throw new SecurityException("author doesn't match");
                }

                if (recipeId1 != recipeId) {
                    throw new IllegalArgumentException("recipeId is wrong");
                }

                return null;
            }, reviewId);

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

        String sql3 = "delete from review_likes where reviewId = ?";
        jdbcTemplate.update(sql3,
                    reviewId
        );

        String sql4 = "delete from reviews where reviewId = ?";
        jdbcTemplate.update(sql4,
                reviewId
        );

        String sql5 = """
                update recipes
                    set
                        reviewCount = (
                            select count(*) from reviews where recipeId = ?
                        ),
                        aggregatedRating = coalesce(
                            (select avg(rating) from reviews where recipeId = ?),
                            0
                        )
                    where recipeId = ?
                """;

        jdbcTemplate.update(sql5, recipeId, recipeId, recipeId);
    }

    @Override
    @Transactional
    public long likeReview(AuthInfo auth, long reviewId) {
        String sql1 = "select authorId from reviews where reviewId = ?";
        String sql2 = "select password, isDeleted from users where AuthorId = ?";

        if (auth == null) {
            throw new SecurityException("auth wrong");
        }

        try {
            jdbcTemplate.queryForObject(sql1, (rs, rowNum) -> {
                long authorId1 = rs.getLong("authorId");

                if (authorId1 == auth.getAuthorId()) {
                    throw new SecurityException("can't like yourself");
                }
                return null;
            }, reviewId);

            try {
                jdbcTemplate.queryForObject(sql2, (rs, rowNum) -> {
                    String password = rs.getString("password");
                    boolean isDeleted = rs.getBoolean("isDeleted");

                    if (!password.equals(auth.getPassword()) || isDeleted) {
                        throw new SecurityException("password wrong or user is deleted");
                    }
                    return null;
                }, auth.getAuthorId());
            } catch (EmptyResultDataAccessException e) {
                throw new SecurityException("user doesn't exist");
            }

        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Recipe does not exist");
        }

        String sql3 = "INSERT INTO review_likes (ReviewId, AuthorId) VALUES (?, ?) ON CONFLICT DO NOTHING";
        jdbcTemplate.update(sql3, reviewId, auth.getAuthorId());

        String sql4 = "select count(*) from review_likes where reviewId = ?";
        return jdbcTemplate.queryForObject(sql4, Long.class, reviewId);
    }

    @Override
    @Transactional
    public long unlikeReview(AuthInfo auth, long reviewId) {
        String sql = "select recipeId from reviews where reviewId = ?";
        String sql1 = "select authorId from review_likes where reviewId = ? and authorId = ?";
        String sql2 = "select password, isDeleted from users where AuthorId = ?";

        if (auth == null) {
            throw new SecurityException("auth wrong");
        }

        try {
            jdbcTemplate.queryForObject(sql, Long.class, reviewId);

        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("review doesn't exist");
        }

        try {
            jdbcTemplate.queryForObject(sql2, (rs, rowNum) -> {
                String password = rs.getString("password");
                boolean isDeleted = rs.getBoolean("isDeleted");

                if (!password.equals(auth.getPassword()) || isDeleted) {
                    throw new SecurityException("password wrong or user is deleted");
                }
                return null;
            }, auth.getAuthorId());
        } catch (EmptyResultDataAccessException e) {
            throw new SecurityException("user doesn't exist");
        }

        try {
            jdbcTemplate.queryForObject(sql1, Long.class, reviewId, auth.getAuthorId());

        } catch (EmptyResultDataAccessException e) {
            return 0;
        }

        String sql3 = "delete from review_likes where AuthorId = ? and ReviewId = ?";
        jdbcTemplate.update(sql3, auth.getAuthorId(), reviewId);

        String sql4 = "select count(*) from review_likes where reviewId = ?";
        return jdbcTemplate.queryForObject(sql4, Long.class, reviewId);
    }

    @Override
    public PageResult<ReviewRecord> listByRecipe(long recipeId, int page, int size, String sort) {
        if (page < 1 || size <= 0) {
            throw new IllegalArgumentException("page < 1 or size <= 0");
        }
        String orderBy = switch (sort) {
            case "date_desc" -> "r.DateModified DESC";
            case "likes_desc" -> "r.reviewCount DESC";
            default -> throw new IllegalArgumentException("Unknown sort: " + sort);
        };

        PageResult<ReviewRecord> reviewRecords = new PageResult<>();
        int offset = (page - 1) * size;
        String sql = """
                select r.ReviewId, r.RecipeId, r.AuthorId, u.authorName,
                Rating, Review, DateSubmitted, DateModified from
                reviews r left join users u on r.authorId = u.authorId where recipeId = ?
                """;

        sql = sql + " order by " + orderBy + " limit ? offset ?;";


        List<ReviewRecord> reviewRecordList = jdbcTemplate.query(
                sql,
                new BeanPropertyRowMapper<>(ReviewRecord.class),
                recipeId,
                size,
                offset
        );
        reviewRecords.setItems(reviewRecordList);

        String sql1 = "select authorId from review_likes where ReviewId = ?";
        for (ReviewRecord reviewRecord : reviewRecords.getItems()) {
            long reviewId = reviewRecord.getReviewId();
            List<Long> likes = jdbcTemplate.query(
                    sql1,
                    (rs, rowNum) -> rs.getLong("authorId"),
                    reviewId
            );
            reviewRecord.setLikes(
                    likes.stream().mapToLong(Long::longValue).toArray()
            );
        }

        String sql2 = "select count(*) from reviews where recipeId = ?";
        long total = jdbcTemplate.queryForObject(sql2, Long.class, recipeId);

        reviewRecords.setTotal(total);
        reviewRecords.setSize(size);
        reviewRecords.setPage(page);

        return reviewRecords;
    }

    @Override
    @Transactional
    public RecipeRecord refreshRecipeAggregatedRating(long recipeId) {
        String sql1 = "select recipeId from reviews where recipeId = ?";
        try {
            jdbcTemplate.queryForObject(sql1, Long.class, recipeId);
        } catch (Exception e) {
            throw new IllegalArgumentException("recipe doesn't exist");
        }

        String sql3 = """
                update recipes
                    set
                        reviewCount = (
                            select count(*) from reviews where recipeId = ?
                        ),
                        aggregatedRating = (select avg(rating) from reviews where recipeId = ?),
                        )
                    where recipeId = ?
                """;

        jdbcTemplate.update(sql3, recipeId, recipeId, recipeId);

        String querySql = """
                    select
                        r.RecipeId, r.Name, r.AuthorId, u.AuthorName,
                        r.CookTime, r.PrepTime, r.TotalTime,
                        r.DatePublished, r.Description, r.RecipeCategory,
                        r.AggregatedRating, r.ReviewCount,
                        r.Calories, r.FatContent, r.SaturatedFatContent,
                        r.CholesterolContent, r.SodiumContent,
                        r.CarbohydrateContent, r.FiberContent, r.SugarContent,
                        r.ProteinContent, r.RecipeServings, r.RecipeYield
                    from recipes r
                    left join users u on r.AuthorId = u.AuthorId
                    where r.RecipeId = ?
                """;

        return jdbcTemplate.queryForObject(
                querySql,
                new BeanPropertyRowMapper<>(RecipeRecord.class),
                recipeId
        );

    }

}