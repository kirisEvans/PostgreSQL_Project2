package io.sustc.benchmark;

import io.fury.ThreadSafeFury;
import io.sustc.dto.*;
import io.sustc.service.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
public class BenchmarkService {

    @Autowired
    private BenchmarkConfig config;

    @Autowired
    private DatabaseService databaseService;

    @Autowired(required = false)
    private UserService userService;

    @Autowired(required = false)
    private RecipeService recipeService;

    @Autowired(required = false)
    private ReviewService reviewService;

    @Autowired
    private ThreadSafeFury fury;

//    private final Map<Long, String> sentDanmu = new ConcurrentHashMap<>();
//
//    private final Set<String> postedVideo = new ConcurrentSkipListSet<>();
//
//    private final Set<Long> registeredUser = new ConcurrentSkipListSet<>();

    @BenchmarkStep(order = 0, description = "Drop all the tables")
    public void drop() {
        if (!config.isStudentMode()) {
            return;
        }
        log.warn("Drop tables");
        databaseService.drop();
    }

    @BenchmarkStep(order = 1, timeout = 35, description = "Import data")
    public BenchmarkResult importData() {
        List<ReviewRecord> reviewRecords = deserialize(BenchmarkConstants.IMPORT_DATA, BenchmarkConstants.REVIEW_RECORDS);
        List<UserRecord> userRecords = deserialize(BenchmarkConstants.IMPORT_DATA, BenchmarkConstants.USER_RECORDS);
        List<RecipeRecord> recipeRecords = deserialize(BenchmarkConstants.IMPORT_DATA, BenchmarkConstants.RECIPE_RECORDS);

        val startTime = System.currentTimeMillis();
        try {
            databaseService.importData(reviewRecords, userRecords, recipeRecords);
        } catch (Exception e) {
            log.error("Exception encountered during importing data, you may early stop this run", e);
        }
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(endTime - startTime);
    }

    @BenchmarkStep(order = 2, description = "Test RecipeService#getRecipeNameFromID(Long)")
    public BenchmarkResult getRecipeNameFromIDTest() {
        Map<Long, String> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.RECIPE_NAME);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.entrySet().forEach(it -> {
            try { 
                val res = recipeService.getNameFromID(it.getKey());
                if (Objects.equals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 3, description = "Test RecipeService#getRecipeById(long)")
    public BenchmarkResult getRecipeByIdTest() {
        Map<Long, RecipeRecord> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.RECIPE_RECORD_SINGLE);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.entrySet().forEach(it -> {
            try {
                val res = recipeService.getRecipeById(it.getKey());
                if (Objects.equals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (IllegalArgumentException illegalArgumentException) {
                if (it.getKey() <= 0) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), "IllegalArgumentException");
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 4, description = "Test RecipeService#searchRecipes(String, String, Double, Integer, Integer, String)")
    public BenchmarkResult searchRecipesTest() {
        List<Map.Entry<Object[], PageResult<RecipeRecord>>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.RECIPE_SEARCH);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            try {
                val args = it.getKey();
                val res = recipeService.searchRecipes((String) args[0], (String) args[1], (Double) args[2], (Integer) args[3], (Integer) args[4], (String) args[5]);
                if (Objects.equals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong search result for args {}: expected {}, got {}", args, it.getValue(), res);
                }
            } catch (IllegalArgumentException illegalArgumentException) {
                if ((Integer)it.getKey()[3] < 1 || (Integer) it.getKey()[4] <= 0) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), "IllegalArgumentException");
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it.getKey(), e);
            }
        });

        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 5, description = "Test RecipeService#createRecipe(RecipeRecord, AuthInfo)")
    public BenchmarkResult createRecipeTest() {
        List<Map.Entry<Object[], Long>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.RECIPE_CREATE);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            val args = it.getKey();
            val dto = (RecipeRecord) args[0];
            val auth = (AuthInfo) args[1];
            try {
                val res = recipeService.createRecipe(dto, auth);
                if (Objects.equals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (IllegalArgumentException illegalArgumentException) {
                if (dto.getRecipeId() <= 0 || dto.getName() == null || Objects.equals(dto.getName(), "")) { // 或者其他非法字段
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), "IllegalArgumentException");
                }
            } catch (SecurityException securityException) {
                if (userService.login(auth) == -1L) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), "SecurityException");
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 6, description = "Test RecipeService#deleteRecipe(long, AuthInfo)")
    public BenchmarkResult deleteRecipeTest() {
        List<Map.Entry<Object[], Boolean>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.RECIPE_DELETE);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            val args = it.getKey();
            val auth = (AuthInfo) args[1];
            val recipe = recipeService.getRecipeById((long) args[0]);
            try {
                recipeService.deleteRecipe((long) args[0], auth);
                val res1 = recipeService.getRecipeById((long) args[0]);
                if (Boolean.TRUE.equals(it.getValue()) && Objects.equals(res1, null)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), true);
                }
            } catch (SecurityException SecurityException) {
                if (!Objects.equals(auth.getAuthorId(), recipe.getAuthorId())) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), "SecurityException");
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it.getKey(), e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 7, description = "Test RecipeService#updateTimes(AuthInfo, long, String, String)")
    public BenchmarkResult updateTimesTest() {
        List<Map.Entry<Object[], String>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.RECIPE_UPDATE_TIMES);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            val args = it.getKey();
            val before = recipeService.getRecipeById((long) args[1]);
            val auth = (AuthInfo) args[0];
            try {
                recipeService.updateTimes(auth, (long) args[1], (String) args[2], (String) args[3]);
                val res = recipeService.getRecipeById((long) args[1]);
                if (Objects.equals(it.getValue(), res.getTotalTime())) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res.getTotalTime());
                }
            } catch (IllegalArgumentException illegalArgumentException) {
                val res = recipeService.getRecipeById((long) args[1]);
                if (Objects.equals(it.getValue(), "illegalArgument")) {
                    // 确认原记录没修改
                    if (Objects.equals(res.getTotalTime(), before.getTotalTime()) && Objects.equals(res.getCookTime(), before.getCookTime()) && Objects.equals(res.getPrepTime(), before.getPrepTime())) {
                        pass.incrementAndGet();
                    } else {
                        log.debug("Wrong answer for {}", it.getKey());
                    }
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), "IllegalArgumentException");
                }
            } catch (SecurityException securityException) {
                val res = recipeService.getRecipeById((long) args[1]);
                if (Objects.equals(it.getValue(), "security")) {
                    // 确认原记录没修改
                    if (Objects.equals(res.getTotalTime(), before.getTotalTime()) && Objects.equals(res.getCookTime(), before.getCookTime()) && Objects.equals(res.getPrepTime(), before.getPrepTime())) {
                        pass.incrementAndGet();
                    } else {
                        log.debug("Wrong answer for {}", it.getKey());
                    }
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), "SecurityException");
                }
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 8, description = "Test RecipeService#getClosestCaloriePair()")
    public BenchmarkResult getClosestCaloriePairTest() {
        Map<String, Object> truth = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.RECIPE_CLOSEST_CALORIE_PAIR);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        val res = recipeService.getClosestCaloriePair();
        boolean same =
                truth.get("RecipeA").equals(res.get("RecipeA")) && truth.get("RecipeB").equals(res.get("RecipeB")) &&
                        Objects.equals(truth.get("CaloriesA"), res.get("CaloriesA")) && Objects.equals(truth.get("CaloriesB"), res.get("CaloriesB")) && Objects.equals(truth.get("Difference"), res.get("Difference"));
        if (same) {
            pass.incrementAndGet();
        } else {
            log.debug("Wrong answer.");
        }
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 9, description = "Test RecipeService#getTop3MostComplexRecipesByIngredients()")
    public BenchmarkResult getTop3MostComplexRecipesByIngredientsTest() {
        List<Map<String, Object>> truth = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.RECIPE_TOP3);
        val pass = new AtomicLong();
        val res = recipeService.getTop3MostComplexRecipesByIngredients();
        val startTime = System.currentTimeMillis();
        if (truth != null && res != null && truth.size() == res.size()) {
            boolean allSame = true;
            for (int i = 0; i < truth.size(); i++) {
                Map<String, Object> t = truth.get(i);
                Map<String, Object> r = res.get(i);
                boolean same = Objects.equals(t.get("RecipeId"), r.get("RecipeId")) && Objects.equals(t.get("Name"), r.get("Name")) && Objects.equals(t.get("IngredientCount"), r.get("IngredientCount"));
                if (!same) {
                    allSame = false;
                    break;
                }
            }
            if (allSame) {
                pass.incrementAndGet();
            } else  {
                log.debug("Wrong answer.");
            }
        } else {
            log.debug("Wrong answer.");
        }
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 10, description = "Test ReviewService#addReview(AuthInfo, long, int, String)")
    public BenchmarkResult addReviewTest() {
        List<Map.Entry<Object[], Object[]>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.REVIEW_ADD);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            val args = it.getKey();
            try {
                long res = reviewService.addReview((AuthInfo) args[0], (long) args[1], (int) args[2], (String) args[3]);
                if (Objects.equals(it.getValue()[0], res)) {
                    // 检查recipe两个字段是否更新
                    if (Objects.equals(recipeService.getRecipeById((long) args[1]), it.getValue()[1])) {
                        pass.incrementAndGet();
                    } else  {
                        log.debug("Wrong answer: wrong aggregated_rating or review_count for recipe {}", it.getKey()[1]);
                    }
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue()[0], res);
                }
            } catch (IllegalArgumentException illegalArgumentException) {
                if (Objects.equals(it.getValue()[0], -1L)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue()[0], "IllegalArgumentException");
                }
            } catch (SecurityException securityException) {
                if (userService.login((AuthInfo) args[0]) == -1L) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue()[0], "SecurityException");
                }
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 11, description = "Test ReviewService#editReview(AuthInfo, long, long, int, String)")
    public BenchmarkResult editReviewTest() {
        List<Map.Entry<Object[], Object[]>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.REVIEW_EDIT);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            val args = it.getKey();
            try {
                reviewService.editReview((AuthInfo) args[0], (long) args[1], (long) args[2], (int) args[3], (String) args[4]);
                if (Objects.equals(it.getValue()[0], "success")) {
                    // 检查recipe两个字段是否更新
                    if (Objects.equals(recipeService.getRecipeById((long) args[1]), it.getValue()[1])) {
                        pass.incrementAndGet();
                    } else  {
                        log.debug("Wrong answer: wrong aggregated_rating or review_count for recipe {}", it.getKey()[1]);
                    }
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue()[0], false);
                }
            } catch (IllegalArgumentException illegalArgumentException) {
                if (Objects.equals(it.getValue()[0], "illegalArgument")) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue()[0], "IllegalArgumentException");
                }
            } catch (SecurityException securityException) {
                if (Objects.equals(it.getValue()[0], "security")) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue()[0], "SecurityException");
                }
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 12, description = "Test ReviewService#deleteReview(AuthInfo, long, long)")
    public BenchmarkResult deleteReviewTest() {
        List<Map.Entry<Object[], Object[]>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.REVIEW_DELETE);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            val args = it.getKey();
            try {
                reviewService.deleteReview((AuthInfo) args[0], (long) args[1], (long) args[2]);
                if (Objects.equals(it.getValue()[0], "success")) {
                    // 检查recipe两个字段是否更新
                    if (Objects.equals(recipeService.getRecipeById((long) args[1]), it.getValue()[1])) {
                        pass.incrementAndGet();
                    } else  {
                        log.debug("Wrong answer: wrong aggregated_rating or review_count for recipe {}", it.getKey()[1]);
                    }
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), false);
                }
            } catch (IllegalArgumentException illegalArgumentException) {
                if (Objects.equals(it.getValue()[0], "illegalArgument")) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue()[0], "IllegalArgumentException");
                }
            } catch (SecurityException securityException) {
                if (Objects.equals(it.getValue()[0], "security")) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue()[0], "SecurityException");
                }
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 13, description = "Test ReviewService#likeReview(AuthInfo, long)")
    public BenchmarkResult likeReviewTest() {
        List<Map.Entry<Object[], Long>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.REVIEW_LIKE);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            val args = it.getKey();
            long reviewId = (long) args[1];
            try {
                val res = reviewService.likeReview((AuthInfo) args[0],  reviewId);
                if (Objects.equals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (IllegalArgumentException illegalArgumentException) {
                if (Objects.equals(it.getValue(), -1L)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), "IllegalArgumentException");
                }
            } catch (SecurityException securityException) {
                if (Objects.equals(it.getValue(), -2L)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), "SecurityException");
                }
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 14, description = "Test ReviewService#unlikeReview(long, long)")
    public BenchmarkResult unlikeReviewTest() {
        List<Map.Entry<Object[], Long>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.REVIEW_UNLIKE);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            val args = it.getKey();
            try {
                val res = reviewService.unlikeReview((AuthInfo) args[0], (long) args[1]);
                if (Objects.equals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (IllegalArgumentException illegalArgumentException) {
                if (Objects.equals(it.getValue(), -1L)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), "IllegalArgumentException");
                }
            } catch (SecurityException securityException) {
                if (Objects.equals(it.getValue(), -2L)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), "SecurityException");
                }
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 15, description = "Test ReviewService#listByRecipe(long, int, int, String)")
    public BenchmarkResult listByRecipeTest() {
        List<Map.Entry<Object[], PageResult<ReviewRecord>>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.REVIEW_LIST);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            val args = it.getKey();
            try {
                val res = reviewService.listByRecipe((long) args[0], (int) args[1], (int) args[2], (String) args[3]);
                if (Objects.equals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong search result for args {}: expected {}, got {}", args, it.getValue(), res);
                }
            } catch (IllegalArgumentException illegalArgumentException) {
                if ((int) args[1] < 1 || (int) args[2] <= 0) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), "IllegalArgumentException");
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it.getKey(), e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 16, description = "Test UserService#register(RegisterUserReq)")
    public BenchmarkResult registerTest() {
        List<Map.Entry<RegisterUserReq, Long>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.USER_REGISTER);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            val args = it.getKey();
            try {
                val res = userService.register(args);
                if (Objects.equals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong register result for {}: expected {}, got {}", args, it.getValue(), res);
                }
            } catch (Exception e) {
                if (Objects.equals(it.getValue(), -1L)) {
                    pass.incrementAndGet();
                } else {
                    log.error("Exception thrown for args {}: {}", args, e.toString());
                }
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 17, description = "Test UserService#follow(AuthInfo, long)")
    public BenchmarkResult followTest() {
        List<Map.Entry<Object[], Boolean>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.USER_FOLLOW);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            val args = it.getKey();
            try {
                val res = userService.follow((AuthInfo) args[0], (long) args[1]);
                if (Boolean.TRUE.equals(it.getValue())) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong result for {}: expected {}, got {}", args, "true or SecurityException", res);
                }
            } catch (SecurityException e) {
                if (Boolean.FALSE.equals(it.getValue())) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), "SecurityException");
                }
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 18, description = "Test UserService#deleteAccount(AuthInfo, long)")
    public BenchmarkResult deleteAccountTest() {
         List<Map.Entry<Object[], Boolean>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.USER_DELETE);
         val pass = new AtomicLong();

         val startTime = System.currentTimeMillis();
         cases.forEach(it -> {
             val args = it.getKey();
             try {
                 val res = userService.deleteAccount((AuthInfo) args[0], (long) args[1]);
                 if (Boolean.TRUE.equals(it.getValue())) {
                     pass.incrementAndGet();
                 } else {
                     log.debug("Wrong result for {}: expected {}, got {}", args, "true or exception", res);
                 }
             } catch (IllegalArgumentException | SecurityException illegalArgumentException) {
                 if (Boolean.FALSE.equals(it.getValue())) {
                     pass.incrementAndGet();
                 } else {
                     log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), "IllegalArgumentException");
                 }
             } catch (Exception e) {
                 log.error("Exception thrown for args {}: {}", Arrays.toString(args), e.toString());
             }
         });
         val endTime = System.currentTimeMillis();

         return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 19, description = "Test UserService#getById(long)")
    public BenchmarkResult getByIdTest() {
        List<Map.Entry<Long, UserRecord>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.USER_GET_BY_ID);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            try {
                val expected = it.getValue();
                val actual = userService.getById(it.getKey());
                if (expected.getAuthorId() == actual.getAuthorId() &&
                        Objects.equals(expected.getAuthorName(), actual.getAuthorName()) &&
                        Objects.equals(expected.getGender(), actual.getGender()) &&
                        expected.getAge() == actual.getAge() &&
                        expected.getFollowers() == actual.getFollowers() &&
                        expected.getFollowing() == actual.getFollowing() &&
                        Objects.equals(expected.getPassword(), actual.getPassword()) &&
                        expected.isDeleted() == actual.isDeleted()) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong getById result for {}: expected {}, got {}", it.getKey(), it.getValue(), actual);
                }
            } catch (Exception e) {
                log.error("Exception thrown for userId {}: {}", it.getKey(), e.toString());
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 20, description = "Test UserService#updateProfile(AuthInfo, String, Integer)")
    public BenchmarkResult updateProfileTest() {
        List<Map.Entry<Object[], UserRecord>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.USER_UPDATE);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            val args = it.getKey();
            val userId = (AuthInfo) args[0];
            try {
                userService.updateProfile((AuthInfo) args[0], (String) args[1], (Integer) args[2]);
                val actual = userService.getById(userId.getAuthorId());
                val expected = it.getValue();
                if (expected.getAuthorId() == actual.getAuthorId() &&
                        Objects.equals(expected.getAuthorName(), actual.getAuthorName()) &&
                        Objects.equals(expected.getGender(), actual.getGender()) &&
                        expected.getAge() == actual.getAge() &&
                        expected.getFollowers() == actual.getFollowers() &&
                        expected.getFollowing() == actual.getFollowing() &&
                        Objects.equals(expected.getPassword(), actual.getPassword()) &&
                        expected.isDeleted() == actual.isDeleted()) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong update result for {}: expected {}, got {}", Arrays.toString(args), it.getValue(), actual);
                }
            } catch (Exception e) {
                if (it.getValue() == null) {
                    pass.incrementAndGet();
                } else {
                    log.error("Exception thrown for {}: {}", Arrays.toString(args), e.toString());
                }
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 21, description = "Test UserService#login(AuthInfo)")
    public BenchmarkResult loginTest() {
        List<Map.Entry<AuthInfo, Long>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.USER_LOGIN);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            val args = it.getKey();
            try {
                val res = userService.login(args);
                if (Objects.equals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong login result for {}: expected {}, got {}", args, it.getValue(), res);
                }
            } catch (Exception e) {
                log.error("Exception thrown for args {}: {}", args, e.toString());
            }
        });
        val endTime = System.currentTimeMillis();
        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 22, description = "Test UserService#feed(AuthInfo, int, int, String)")
    public BenchmarkResult feedTest() {
        List<Map.Entry<Object[], PageResult<FeedItem>>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.USER_FEED);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            val args = it.getKey();
            try {
                val res = userService.feed((AuthInfo) args[0], (int) args[1], (int) args[2], (String) args[3]);
                if (Objects.equals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong feed result for args {}: expected {}, got {}", Arrays.toString(args), it.getValue(), res);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}: {}", Arrays.toString(args), e.toString());
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 23, description = "Test RecipeService#getUserWithHighestFollowRatio")
    public BenchmarkResult getUserWithHighestFollowRatioTest() {
        Map<String, Object> truth = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.USER_HIGHEST_FOLLOW_RATIO);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        val res = userService.getUserWithHighestFollowRatio();
        boolean same =
                Objects.equals(truth.get("AuthorId"),   res.get("AuthorId")) &&
                        Objects.equals(truth.get("AuthorName"), res.get("AuthorName")) && Math.abs((Double) truth.get("Ratio") - (Double) res.get("Ratio"))< 1e-9;
        if (same) {
            pass.incrementAndGet();
        } else {
            log.debug("Wrong answer.");
        }
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    private <T> T deserialize(String... path) {
        val file = Paths.get(config.getDataPath(), path);
        return (T) fury.deserialize(Files.readAllBytes(file));
    }

    private static boolean collectionEquals(Collection<?> expect, Collection<?> actual) {
        return Objects.equals(expect, actual)
                || expect.isEmpty() && Objects.isNull(actual);
    }

    private static boolean longArrayAsSetEquals(long[] expect, long[] actual) {
        if (expect.length != actual.length) {
            return false;
        }
        val expectSet = new HashSet<Long>();
        for (val i : expect) {
            expectSet.add(i);
        }
        for (val i : actual) {
            if (!expectSet.remove(i)) {
                return false;
            }
        }
        return expectSet.isEmpty();
    }

    private static <T> boolean arrayAsSetEquals(T[] expect, T[] actual) {
        if (expect.length != actual.length) {
            return false;
        }
        return Objects.equals(new HashSet<>(Arrays.asList(expect)), new HashSet<>(Arrays.asList(actual)));
    }

//    private static boolean userInfoEquals(UserInfoResp expect, UserInfoResp actual) {
//        return Objects.isNull(expect) == Objects.isNull(actual)
//                && expect.getMid() == actual.getMid()
//                && expect.getCoin() == actual.getCoin()
//                && longArrayAsSetEquals(expect.getFollowing(), actual.getFollowing())
//                && longArrayAsSetEquals(expect.getFollower(), actual.getFollower())
//                && arrayAsSetEquals(expect.getWatched(), actual.getWatched())
//                && arrayAsSetEquals(expect.getLiked(), actual.getLiked())
//                && arrayAsSetEquals(expect.getCollected(), actual.getCollected())
//                && arrayAsSetEquals(expect.getPosted(), actual.getPosted());
//    }
}
