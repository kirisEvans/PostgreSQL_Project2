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


    @Override
    public String getNameFromID(long id) {
        return null;
    }

    @Override
    public RecipeRecord getRecipeById(long recipeId) {
        return null;
    }


    @Override
    public PageResult<RecipeRecord> searchRecipes(String keyword, String category, Double minRating,
                                                  Integer page, Integer size, String sort) {
        return null;
    }

    @Override
    public long createRecipe(RecipeRecord dto, AuthInfo auth) {
        return 0;
    }

    @Override
    public void deleteRecipe(long recipeId, AuthInfo auth) {

    }

    @Override
    public void updateTimes(AuthInfo auth, long recipeId, String cookTimeIso, String prepTimeIso) {
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