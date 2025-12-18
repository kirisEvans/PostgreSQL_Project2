package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.RecipeService;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class RecipeServiceImpl implements RecipeService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public String getNameFromID(long id) {
        String sql = "select Name from recipes where RecipeId = ?";
        try {
            return jdbcTemplate.queryForObject(sql, String.class, id);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public RecipeRecord getRecipeById(long recipeId) {
        if (recipeId <= 0) throw new IllegalArgumentException("recipeId must be positive");
        String sql1 = """
                SELECT
                    r.RecipeId,
                    r.Name,
                    r.AuthorId,
                    u.AuthorName,
                    r.CookTime,
                    r.PrepTime,
                    r.TotalTime,
                    r.DatePublished,
                    r.Description,
                    r.RecipeCategory,
                    r.AggregatedRating,
                    r.ReviewCount,
                    r.Calories,
                    r.FatContent,
                    r.SaturatedFatContent,
                    r.CholesterolContent,
                    r.SodiumContent,
                    r.CarbohydrateContent,
                    r.FiberContent,
                    r.SugarContent,
                    r.ProteinContent,
                    r.RecipeServings,
                    r.RecipeYield
                FROM recipes r
                LEFT JOIN users u ON r.AuthorId = u.AuthorId
            WHERE r.RecipeId = ?;
            """;

        RecipeRecord recipe;
        try {
            recipe = jdbcTemplate.queryForObject(
                    sql1,
                    new BeanPropertyRowMapper<>(RecipeRecord.class),
                    recipeId
            );
        } catch (EmptyResultDataAccessException e) {
            return null;
        }

        String sql2 = "SELECT IngredientPart FROM recipe_ingredients WHERE RecipeId = ? order by IngredientPart";
        List<String> ingredientParts = jdbcTemplate.query(
                sql2,
                (rs, rowNum) -> rs.getString("IngredientPart"),
                recipeId
        );

        recipe.setRecipeIngredientParts(ingredientParts.toArray(new String[0]));

        return recipe;
    }


    @Override
    public PageResult<RecipeRecord> searchRecipes(String keyword, String category, Double minRating,
                                                  Integer page, Integer size, String sort) {
        if (page < 1 || size <= 0) throw new IllegalArgumentException("page must be bigger than 1 or size must be positive");

        String keywordPattern = "%" + keyword + "%";
        String orderBy = switch (sort) {
            case "date_desc" -> "r.DatePublished DESC";
            case "rating_desc" -> "r.AggregatedRating DESC, r.RecipeId desc";
            case "calories_asc" -> "r.Calories ASC, r.RecipeId desc";
            default -> throw new IllegalArgumentException("Unknown sort: " + sort);
        };
        PageResult<RecipeRecord> recipeRecords = new PageResult<>();
        int offset = (page - 1) * size;
        List<Object> params = new ArrayList<>();
        List<Object> countParams = new ArrayList<>();

        StringBuilder sql1 = new StringBuilder("""
            SELECT r.RecipeId, r.Name, r.AuthorId, u.AuthorName, r.CookTime, r.PrepTime, r.TotalTime,
                   r.DatePublished, r.Description, r.RecipeCategory, r.AggregatedRating, r.ReviewCount,
                   r.Calories, r.FatContent, r.SaturatedFatContent, r.CholesterolContent, r.SodiumContent,
                   r.CarbohydrateContent, r.FiberContent, r.SugarContent, r.ProteinContent, r.RecipeServings, r.RecipeYield
            FROM recipes r
            LEFT JOIN users u ON r.AuthorId = u.AuthorId
            WHERE 1=1
        """);

        if (keyword != null) {
            sql1.append(" AND (r.name ilike ? OR r.description ilike ?)");
            params.add(keywordPattern);
            params.add(keywordPattern);
        }

        if (category != null) {
            sql1.append(" AND r.RecipeCategory = ?");
            params.add(category);
        }

        if (minRating != null) {
            sql1.append(" AND r.AggregatedRating >= ?");
            params.add(minRating);
        }

        sql1.append(" ORDER BY ").append(orderBy);
        sql1.append(" LIMIT ? OFFSET ?");
        params.add(size);
        params.add(offset);

        try {
            List<RecipeRecord> recipeList = jdbcTemplate.query(
                    sql1.toString(),
                    new BeanPropertyRowMapper<>(RecipeRecord.class),
                    params.toArray()
            );
            recipeRecords.setItems(recipeList);
        } catch (EmptyResultDataAccessException e) {
            recipeRecords.setItems(null);
        }

        String sql2 = "SELECT IngredientPart FROM recipe_ingredients WHERE RecipeId = ?";
        if (recipeRecords.getItems() != null && !recipeRecords.getItems().isEmpty()) {
            for (RecipeRecord record : recipeRecords.getItems()) {
                long recipeId = record.getRecipeId();
                List<String> ingredientParts = jdbcTemplate.query(
                        sql2,
                        (rs, rowNum) -> rs.getString("IngredientPart"),
                        recipeId
                );
                ingredientParts.sort(String.CASE_INSENSITIVE_ORDER);
                record.setRecipeIngredientParts(ingredientParts.toArray(new String[0]));
            }
        }

        recipeRecords.setPage(page);
        recipeRecords.setSize(size);

        StringBuilder countSql = new StringBuilder(
                "SELECT count(*) FROM recipes r WHERE 1=1"
        );

        if (keyword != null) {
            countSql.append(" AND (r.name ilike ? OR r.description ilike ?)");
            countParams.add(keywordPattern);
            countParams.add(keywordPattern);
        }

        if (category != null) {
            countSql.append(" AND r.RecipeCategory = ?");
            countParams.add(category);
        }
        if (minRating != null) {
            countSql.append(" AND r.AggregatedRating >= ?");
            countParams.add(minRating);
        }

        int total = jdbcTemplate.queryForObject(countSql.toString(), Integer.class, countParams.toArray());
        recipeRecords.setTotal(total);
        return recipeRecords;
    }

    @Override
    public long createRecipe(RecipeRecord dto, AuthInfo auth) {
        String sql1 = "select max(RecipeId) from recipes";
        Long max_id = jdbcTemplate.queryForObject(sql1, Long.class);
        if (max_id == null) {
            max_id = 1L;
        }
        max_id++;

        String sql2 = "SELECT password FROM users WHERE AuthorId = ?";
        try {
            String dbPassword = jdbcTemplate.queryForObject(sql2, String.class, auth.getAuthorId());

            if (!auth.getPassword().equals(dbPassword)) {
                return -1;
            }
        } catch (EmptyResultDataAccessException e) {
            return -1;
        }

        String sql3 = """
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

        if (dto.getRecipeId() != max_id) {
            return -1;
        }
        jdbcTemplate.update(sql3,
                dto.getRecipeId(),
                dto.getName(),
                dto.getAuthorId(),
                dto.getCookTime(),
                dto.getPrepTime(),
                dto.getTotalTime(),
                dto.getDatePublished(),
                dto.getDescription(),
                dto.getRecipeCategory(),
                dto.getAggregatedRating(),
                dto.getReviewCount(),
                dto.getCalories(),
                dto.getFatContent(),
                dto.getSaturatedFatContent(),
                dto.getCholesterolContent(),
                dto.getSodiumContent(),
                dto.getCarbohydrateContent(),
                dto.getFiberContent(),
                dto.getSugarContent(),
                dto.getProteinContent(),
                dto.getRecipeServings(),
                dto.getRecipeYield()
        );

        return max_id;
    }

    @Override
    public void deleteRecipe(long recipeId, AuthInfo auth) {
        String sql1 = "SELECT AuthorId FROM recipes WHERE RecipeId = ?";
        Long authorId;
        try {
            authorId = jdbcTemplate.queryForObject(sql1, Long.class, recipeId);
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("recipe does not exist");
        }

        if (!authorId.equals(auth.getAuthorId())) {
            throw new SecurityException("not recipe author");
        }

        jdbcTemplate.update(
                "delete from review_likes where ReviewId in (select ReviewId FROM reviews WHERE RecipeId = ?)",
                recipeId
        );

        jdbcTemplate.update(
                "DELETE FROM reviews WHERE RecipeId = ?",
                recipeId
        );

        jdbcTemplate.update(
                "DELETE FROM recipe_ingredients WHERE RecipeId = ?",
                recipeId
        );

        int rows = jdbcTemplate.update(
                "DELETE FROM recipes WHERE RecipeId = ?",
                recipeId
        );

        if (rows == 0) {
            throw new IllegalArgumentException("recipe does not exist");
        }
    }

    @Override
    public void updateTimes(AuthInfo auth, long recipeId, String cookTimeIso, String prepTimeIso) {
        String sql1 = "SELECT AuthorId, CookTime, PrepTime FROM recipes WHERE RecipeId = ?";
        RecipeRecord r;
        try {
            r = jdbcTemplate.queryForObject(sql1, new BeanPropertyRowMapper<>(RecipeRecord.class), recipeId);
        } catch (EmptyResultDataAccessException e) {
            throw new SecurityException("recipe does not exist");
        }

        if (r != null && r.getAuthorId() != auth.getAuthorId()) {
            throw new SecurityException("not recipe author");
        }

        if (!(isValidDuration(cookTimeIso) && isValidDuration(prepTimeIso))) {
            throw new IllegalArgumentException("text wrong");
        }

        String sql2 = "update recipes set CookTime = ?, PrepTime = ?, totalTime = ? where RecipeId = ?";
        String cookTimeFinal = null;
        String prepTimeFinal = null;

        if (r != null) {
            cookTimeFinal = (cookTimeIso != null && !cookTimeIso.isEmpty()) ? cookTimeIso : (r.getCookTime() != null &&
                    !r.getCookTime().isEmpty() ? r.getCookTime() : "PT0S");
        }
        if (r != null) {
            prepTimeFinal = (prepTimeIso != null && !prepTimeIso.isEmpty()) ? prepTimeIso : (r.getPrepTime() != null &&
                    !r.getPrepTime().isEmpty() ? r.getPrepTime() : "PT0S");
        }

        if (cookTimeFinal != null && (Duration.parse(cookTimeFinal).isNegative() || Duration.parse(prepTimeFinal).isNegative())) {
            throw new IllegalArgumentException("text wrong");
        }

        try {
            if (cookTimeFinal != null) {
                jdbcTemplate.update(sql2,
                        cookTimeFinal,
                        prepTimeFinal,
                        Duration.parse(cookTimeFinal).plus(Duration.parse(prepTimeFinal)).toString(),
                        recipeId
                );
            }
        } catch (Exception e) {
            throw new NullPointerException();
        }
    }

    private boolean isValidDuration(String text) {
        if (text == null) return true;
        try {
            Duration.parse(text);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    @Override
    public Map<String, Object> getClosestCaloriePair() {
        return null;
    }

    @Override
    public List<Map<String, Object>> getTop3MostComplexRecipesByIngredients() {
        return null;
    }


}