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

        String sql3 = "select isDeleted from users where authorId = ?";
        if (jdbcTemplate.queryForObject(sql3, boolean.class, recipe.getAuthorId())) {
            return null;
        }
        return recipe;
    }


    @Override
    public PageResult<RecipeRecord> searchRecipes(String keyword, String category, Double minRating,
                                                  Integer page, Integer size, String sort) {
        if (page < 1 || size <= 0) throw new IllegalArgumentException("page must be bigger than 1 or size must be positive");

        String keywordPattern = "%" + keyword + "%";

        if (sort == null) {
            sort = "date_desc";
        }

        String orderBy = switch (sort) {
            case "date_desc" -> "r.DatePublished DESC, r.RecipeId desc";
            case "rating_desc" -> "r.AggregatedRating DESC, r.RecipeId desc";
            case "calories_asc" -> "r.Calories ASC, r.RecipeId desc";
            default -> "r.RecipeId desc";
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
        RecipeRecord r;
        if (auth == null) {
            throw new SecurityException("no auth");
        }

        try {
            r = jdbcTemplate.queryForObject(
                    "SELECT AuthorId, CookTime, PrepTime FROM recipes WHERE RecipeId = ?",
                    new BeanPropertyRowMapper<>(RecipeRecord.class),
                    recipeId
            );
        } catch (EmptyResultDataAccessException e) {
            throw new SecurityException("recipe does not exist");
        }

        if (r.getAuthorId() != auth.getAuthorId()) {
            throw new SecurityException("not recipe author");
        }

        Map<String, Object> user = jdbcTemplate.queryForMap(
                "SELECT password, IsDeleted FROM users WHERE AuthorId = ?",
                r.getAuthorId()
        );
        boolean isDeleted = (boolean) user.get("IsDeleted");

        if (isDeleted || !user.get("password").equals(auth.getPassword())) {
            throw new SecurityException("no author");
        }

        if (cookTimeIso != null && cookTimeIso.isBlank()) cookTimeIso = null;
        if (prepTimeIso != null && prepTimeIso.isBlank()) prepTimeIso = null;

        if (!isValidDuration(cookTimeIso) || !isValidDuration(prepTimeIso)) {
            throw new IllegalArgumentException("Invalid duration format");
        }

        String cookTimeFinal =
                (cookTimeIso != null)
                        ? cookTimeIso
                        : normalizeDuration(r.getCookTime());

        String prepTimeFinal =
                (prepTimeIso != null)
                        ? prepTimeIso
                        : normalizeDuration(r.getPrepTime());

        if (!isValidDuration(cookTimeFinal) || !isValidDuration(prepTimeFinal)) {
            throw new IllegalArgumentException("Invalid duration format");
        }

        Duration cookDuration = Duration.parse(cookTimeFinal);
        Duration prepDuration = Duration.parse(prepTimeFinal);
        if (cookDuration.isNegative() || prepDuration.isNegative()) {
            throw new IllegalArgumentException("Duration cannot be negative");
        }

        Duration totalDuration;
        try {
            totalDuration = cookDuration.plus(prepDuration);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Total duration overflow", e);
        }

        try {
            jdbcTemplate.update(
                    "UPDATE recipes SET CookTime = ?, PrepTime = ?, totalTime = ? WHERE RecipeId = ?",
                    cookTimeFinal,
                    prepTimeFinal,
                    totalDuration.toString(),
                    recipeId
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to update recipe times", e);
        }
    }

    private boolean isValidDuration(String text) {
        if (text == null) return true;
        if (text.isBlank()) return false;
        try {
            Duration.parse(text);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    private String normalizeDuration(String text) {
        if (text == null || text.isBlank()) {
            return "PT0S";
        }
        return text;
    }

    @Override
    public Map<String, Object> getClosestCaloriePair() {
        String sql = """
            select r1.recipeId as RecipeA,
                            r2.recipeId as RecipeB,
                            r1.calories as CaloriesA,
                            r2.calories as CaloriesB,
                            abs(r1.calories - r2.calories) as diff
                    from recipes r1 inner join recipes r2
                        on r1.calories = r2.calories + (select min(Difference) from
                    (select abs(calories - lag(calories) over (order by calories)) as Difference from recipes) as rD)
                        and r1.recipeId != r2.recipeId
                    order by r1.recipeId, r2.recipeId desc
                        limit 1;
        """;

        Map<String, Object> map = new HashMap<>();

        try {
            jdbcTemplate.query(sql, rs -> {
                if (rs.next()) {
                    map.put("RecipeA", rs.getLong("RecipeA"));
                    map.put("RecipeB", rs.getLong("RecipeB"));
                    map.put("CaloriesA", rs.getDouble("CaloriesA"));
                    map.put("CaloriesB", rs.getDouble("CaloriesB"));
                    map.put("Difference", rs.getDouble("diff"));
                }
                return map;
            });
        } catch (EmptyResultDataAccessException e) {
            return null;
        }

        return map;
    }

    @Override
    public List<Map<String, Object>> getTop3MostComplexRecipesByIngredients() {
        String sql = """
            select c.recipeId as RecipeId, r.name as Name, cnt as IngredientCount from recipes r inner join
                (select RecipeId, count(*) as cnt from recipe_ingredients
                group by RecipeId order by cnt desc, recipeId limit 3) as c
                on r.recipeId = c.recipeId;
        """;
        List<Map<String, Object>> map;
        try {
            map = jdbcTemplate.query(sql, (rs, rowNum) -> {
                Map<String, Object> m = new HashMap<>();
                m.put("RecipeId", rs.getLong("RecipeId"));
                m.put("Name", rs.getString("Name"));
                m.put("IngredientCount", rs.getInt("IngredientCount"));
                return m;
            });
        } catch (EmptyResultDataAccessException e) {
            return Collections.emptyList();
        }
        return map;
    }
}