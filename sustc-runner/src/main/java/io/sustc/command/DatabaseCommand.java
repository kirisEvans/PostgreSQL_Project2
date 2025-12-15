package io.sustc.command;

import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180ParserBuilder;
import io.fury.ThreadSafeFury;
import io.sustc.benchmark.BenchmarkConfig;
import io.sustc.benchmark.BenchmarkConstants;
import io.sustc.benchmark.BenchmarkService;
import io.sustc.dto.*;
import io.sustc.service.DatabaseService;
import io.sustc.service.RecipeService;
import io.sustc.service.ReviewService;
import io.sustc.service.UserService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import java.nio.file.Files;
import java.nio.file.Paths;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

//在Spring Shell应用中Java类需要使用注解@ShellComponent来修饰，
//类中的方法使用注解@ShellMethod表示为一个具体的命令。
@Slf4j
@ShellComponent
@ConditionalOnBean(DatabaseService.class)
public class DatabaseCommand {

    @Autowired
    private DatabaseService databaseService;

    @Autowired
    private BenchmarkService benchmarkService;

    @Autowired
    private RecipeService recipeService;

    @Autowired
    private ReviewService reviewService;

    @Autowired
    private UserService userService;

    @Autowired
    private ThreadSafeFury fury;

    @Autowired
    private BenchmarkConfig config;

    @ShellMethod(key = "db groupmember", value = "List group members")
    public List<Integer> listGroupMembers() {
        return databaseService.getGroupMembers();
    }

    @ShellMethod(key = "db import", value = "Drop all the tables. Then import data from csv")
    public void importData() {
        long startTime = System.currentTimeMillis();

        databaseService.drop();
        benchmarkService.importData();

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        System.out.println("importData time: " + duration + " ms");
    }

    @ShellMethod(key = "db drop", value = "Drop all the tables")
    public void drop() {
        databaseService.drop();
    }

    @ShellMethod(key = "db sum", value = "Demonstrate using DataSource")
    public Integer sum(int a, int b) {
        return databaseService.sum(a, b);
    }

    @ShellMethod(key = "db csv2ser", value = "Generate .ser files from .csv files")
    public void csv2ser() {
        try {
            // 获取项目根目录
            String projectRoot = System.getProperty("user.dir");
            System.out.println("projectRoot:" + projectRoot);

            List<UserRecord> users = loadUsers(projectRoot + "/data/csv/users.csv");
            List<RecipeRecord> recipes = loadRecipes(projectRoot + "/data/csv/recipes.csv");
            List<ReviewRecord> reviews = loadReviews(projectRoot + "/data/csv/reviews.csv");

            // 序列化数据
            serializeData(users, projectRoot + "/data/import/users.ser");
            serializeData(recipes, projectRoot + "/data/import/recipes.ser");
            serializeData(reviews, projectRoot + "/data/import/reviews.ser");

            System.out.println("Data loading and serialization have been completed!");
            System.out.println("user count: " + users.size());
            System.out.println("recipe count: " + recipes.size());
            System.out.println("review count: " + reviews.size());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String[] parseCsvList(String listStr) {
        if (listStr == null || listStr.trim().isEmpty() || "null".equalsIgnoreCase(listStr.trim())) {
            return new String[0];
        }

        String trimmed = listStr.trim();

        // 检查是否是 c("item1", "item2", ...) 格式
        if (trimmed.startsWith("c(") && trimmed.endsWith(")")) {
            // 提取括号内的内容
            String content = trimmed.substring(2, trimmed.length() - 1).trim();

            // 使用更简单的正则表达式匹配所有引号内的内容
            Pattern pattern = Pattern.compile("\"([^\"]*)\"");
            Matcher matcher = pattern.matcher(content);

            List<String> items = new ArrayList<>();
            while (matcher.find()) {
                items.add(matcher.group(1));
            }

            // 如果找到了引号内的项目，返回它们
            if (!items.isEmpty()) {
                return items.toArray(new String[0]);
            }

            // 如果没有找到引号内容，尝试按逗号分割括号内的内容
            return Arrays.stream(content.split("\\s*,\\s*"))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toArray(String[]::new);
        }

        // 如果不是 c(...) 格式，尝试直接按逗号分割
        return Arrays.stream(trimmed.split("\\s*,\\s*"))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toArray(String[]::new);
    }

    // 辅助方法：解析("123", "456")格式的字符串为long数组
    private static long[] parseCsvLongList(String listStr) {
        // 处理空值或空字符串
        if (listStr == null || listStr.trim().isEmpty() || "null".equalsIgnoreCase(listStr.trim())) {
            return new long[0];
        }

        String trimmedStr = listStr.trim();
        // 移除开头的括号和结尾的引号“
        if(trimmedStr.length()>=2) {
            trimmedStr = trimmedStr.substring(1, trimmedStr.length() - 1);
        }

        // 如果字符串已经是空字符串，返回空数组
        if (trimmedStr.isEmpty()) {
            return new long[0];
        }

        // 使用逗号分割字符串，并去除每个部分的前后空格
        String[] stringArray = trimmedStr.split("\\s*,\\s*");
        long[] longArray = new long[stringArray.length];

        for (int i = 0; i < stringArray.length; i++) {
            try {
                longArray[i] = Long.parseLong(stringArray[i].trim());
            } catch (NumberFormatException e) {
                longArray[i] = 0L; // 解析失败设为0
            }
        }

        return longArray;
    }

    // 辅助方法：解析时间戳
    private static Timestamp parseTimestamp(String timestampStr) {
        if (timestampStr == null || timestampStr.trim().isEmpty() || "null".equalsIgnoreCase(timestampStr.trim())) {
            return null;
        }

        // 尝试多种日期格式
        String[] dateFormats = {"yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd", "MM/dd/yyyy HH:mm:ss", "MM/dd/yyyy"};

        for (String format : dateFormats) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(format);
                Date date = sdf.parse(timestampStr.trim());
                return new Timestamp(date.getTime());
            } catch (ParseException e) {
                // 尝试下一种格式
            }
        }

        return null; // 所有格式都解析失败
    }

    // 辅助方法：解析浮点数，处理空值和异常
    private static float parseFloat(String floatStr) {
        if (floatStr == null || floatStr.trim().isEmpty() || "null".equalsIgnoreCase(floatStr.trim())) {
            return 0.0f;
        }

        try {
            return Float.parseFloat(floatStr.trim());
        } catch (NumberFormatException e) {
            return 0.0f;
        }
    }

    // 辅助方法：解析整数，处理空值和异常
    private static int parseInt(String intStr) {
        if (intStr == null || intStr.trim().isEmpty() || "null".equalsIgnoreCase(intStr.trim())) {
            return 0;
        }

        try {
            return Integer.parseInt(intStr.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    // 辅助方法：解析长整型，处理空值和异常
    private static long parseLong(String longStr) {
        if (longStr == null || longStr.trim().isEmpty() || "null".equalsIgnoreCase(longStr.trim())) {
            return 0L;
        }

        try {
            return Long.parseLong(longStr.trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    // 加载用户数据
    public static List<UserRecord> loadUsers(String filePath) throws IOException, CsvException {
        List<UserRecord> users = new ArrayList<>();

        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            List<String[]> records = reader.readAll();

            // 跳过标题行
            for (int i = 1; i < records.size(); i++) {
                String[] fields = records.get(i);

                if (fields.length >= 9) {
                    UserRecord user = UserRecord.builder().authorId(parseLong(fields[0])).authorName(fields[1] != null ? fields[1].trim() : "").gender(fields[2] != null ? fields[2].trim() : "").age(parseInt(fields[3])).
                            followers(parseInt(fields[4])).
                            following(parseInt(fields[5])).
                            followerUsers(parseCsvLongList(fields[6])).
                            followingUsers(parseCsvLongList(fields[7])).
                            password(fields[8] != null ? fields[8].trim() : "").build();

                    users.add(user);
                }
            }
        }

        return users;
    }

    //加载食谱数据
    public static List<RecipeRecord> loadRecipes(String filePath) throws IOException, CsvException {
        List<RecipeRecord> recipes = new ArrayList<>();

        // 使用 RFC4180Parser 创建 CSVReader
        try (CSVReader reader = new CSVReaderBuilder(new FileReader(filePath))
                .withCSVParser(new RFC4180ParserBuilder().build())
                .build()) {

            List<String[]> records = reader.readAll();

            // 跳过标题行
            for (int i = 1; i < records.size(); i++) {
                String[] fields = records.get(i);

                if (fields.length >= 24) {
                    RecipeRecord recipe = RecipeRecord.builder()
                            .RecipeId(parseLong(fields[0]))
                            .name(fields[1] != null ? fields[1].trim() : "")
                            .authorId(parseLong(fields[2]))
                            .authorName(fields[3] != null ? fields[3].trim() : "")
                            .cookTime(fields[4] != null ? fields[4].trim() : "")
                            .prepTime(fields[5] != null ? fields[5].trim() : "")
                            .totalTime(fields[6] != null ? fields[6].trim() : "")
                            .datePublished(parseTimestamp(fields[7]))
                            .description(fields[8] != null ? fields[8].trim() : "")
                            .recipeCategory(fields[9] != null ? fields[9].trim() : "")
                            .recipeIngredientParts(parseCsvList(fields[10]))
                            .aggregatedRating(parseFloat(fields[11]))
                            .reviewCount((int)parseFloat(fields[12]))
                            .calories(parseFloat(fields[13]))
                            .fatContent(parseFloat(fields[14]))
                            .saturatedFatContent(parseFloat(fields[15]))
                            .cholesterolContent(parseFloat(fields[16]))
                            .sodiumContent(parseFloat(fields[17]))
                            .carbohydrateContent(parseFloat(fields[18]))
                            .fiberContent(parseFloat(fields[19]))
                            .sugarContent(parseFloat(fields[20]))
                            .proteinContent(parseFloat(fields[21]))
                            .recipeServings((int)parseFloat(fields[22]))
                            .recipeYield(fields[23] != null ? fields[23].trim() : "")
                            .build();

                    recipes.add(recipe);
                }
            }
        }

        return recipes;
    }

    // 加载评论数据
    public static List<ReviewRecord> loadReviews(String filePath) throws IOException, CsvException {
        List<ReviewRecord> reviews = new ArrayList<>();

        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            List<String[]> records = reader.readAll();

            // 跳过标题行
            for (int i = 1; i < records.size(); i++) {
                String[] fields = records.get(i);

                if (fields.length >= 9) {
                    ReviewRecord review = ReviewRecord.builder().reviewId(parseLong(fields[0])).
                            recipeId(parseLong(fields[1])).authorId(parseLong(fields[2])).
                            authorName(fields[3] != null ? fields[3].trim() : "").
                            rating(parseFloat(fields[4])).review(fields[5] != null ? fields[5].trim() : "").
                            dateSubmitted(parseTimestamp(fields[6])).dateModified(parseTimestamp(fields[7])).
                            likes(parseCsvLongList(fields[8])).build();

                    reviews.add(review);
                }
            }
        }

        return reviews;
    }

    @SneakyThrows
    public void serializeData(List<?> data, String outputFilePath) throws IOException {
        byte[] serializedData = fury.serialize(data);
        Files.write(Paths.get(outputFilePath), serializedData);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public <T> void  serialize(T object, String... path) {
        // 获取文件路径
        var file = Paths.get(config.getDataPath(), path);

        // 序列化对象
        byte[] serializedData = fury.serialize(object);

        // 确保目录存在
        Files.createDirectories(file.getParent());

        // 将序列化后的数据写入文件
        Files.write(file, serializedData);

        log.info("serialize path {}", file);
    }


}