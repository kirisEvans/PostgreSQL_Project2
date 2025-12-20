package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class UserServiceImpl implements UserService {
    @Autowired
    private JdbcTemplate jdbcTemplate;
    private static final String SALT = "mySecretSalt";

    @Override
    public long register(RegisterUserReq req) {
        String sql1 = "select count(*) from users where AuthorName = ?";
        int count = jdbcTemplate.queryForObject(sql1, Integer.class, req.getName());

        if (req.getGender() == null || req.getGender() == RegisterUserReq.Gender.UNKNOWN
        || req.getName() == null || LocalDate.now().isBefore(LocalDate.parse(req.getBirthday()))
        || count != 0) {
            return -1;
        }

        String sql2 = "select max(AuthorId) from users";
        Long authorId = jdbcTemplate.queryForObject(sql2, Long.class);
        if (authorId == null) {
            authorId = 0L;
        }

        Period period = Period.between(LocalDate.parse(req.getBirthday()), LocalDate.now());

        String sql3 = "INSERT INTO users " +
                "(AuthorId, AuthorName, Gender, Age, Followers, Following, Password, IsDeleted) " +
                "VALUES (?, ?, ?, ?, 0, 0, ?, false)";

        jdbcTemplate.update(sql3,
                authorId+1,
                req.getName(),
                capitalize(req.getGender().name()),
                period.getYears(),
                req.getPassword()
        );
      return authorId+1;
    }

    private static String capitalize(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1).toLowerCase();
    }

    @Override
    public long login(AuthInfo auth) {
        String sql1 = "select IsDeleted, password from users where AuthorId = ?";
        Map<String, Object> userMap;
        try {
            userMap = jdbcTemplate.queryForMap(sql1, auth.getAuthorId());
        } catch (Exception e) {
            return -1;
        }

        boolean IsDeleted = (Boolean) userMap.get("IsDeleted");
        String password = (String) userMap.get("password");

        if (IsDeleted || !hashPassword(auth.getPassword()).equals(hashPassword(password))) {
            return -1;
        }

       return auth.getAuthorId();
    }

    private static String hashPassword(String password) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            String salted = SALT + password;
            byte[] hash = md.digest(salted.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean deleteAccount(AuthInfo auth, long userId) {
        if (auth == null || auth.getAuthorId() != userId) {
            throw new SecurityException("not author");
        }

        try {
            String sql1 = "select IsDeleted from users where AuthorId = ?";
            String sql2 = "update users set IsDeleted = true where AuthorId = ?";
            boolean isDeleted = jdbcTemplate.queryForObject(sql1, Boolean.class, auth.getAuthorId());

            if (isDeleted) {
                return false;
            } else {
                jdbcTemplate.update(sql2, auth.getAuthorId());
            }
        } catch (Exception e) {
            throw new SecurityException("user doesn't exist");
        }

        String sql3 = "delete from user_follows where FollowerId = ? or FollowingId = ?";
        jdbcTemplate.update(sql3, userId, userId);

        return true;
    }

    @Override
    public boolean follow(AuthInfo auth, long followeeId) {
        if (auth == null || auth.getAuthorId() == followeeId) {
            throw new SecurityException("not author or repeat");
        }

        try {
            String sql1 = "select IsDeleted from users where AuthorId = ?";
            boolean isDeleted1 = jdbcTemplate.queryForObject(sql1, Boolean.class, auth.getAuthorId());
            boolean isDeleted2 = jdbcTemplate.queryForObject(sql1, Boolean.class, followeeId);

            if (isDeleted1 || isDeleted2) {
                return false;
            }
        } catch (Exception e) {
            throw new SecurityException("user doesn't exist");
        }

        String sql2 = "select FollowingId from user_follows where FollowerId = ? and FollowingId = ?";

        try {
            jdbcTemplate.queryForObject(sql2, Long.class, auth.getAuthorId());
            String sql3 = "delete from user_follows where FollowerId = ? and FollowingId = ?";
            jdbcTemplate.update(sql3, auth.getAuthorId(), followeeId);

            return true;
        } catch (Exception e) {
            String sql4 = "INSERT INTO user_follows(FollowerId, FollowingId) VALUES (?, ?) ON CONFLICT DO NOTHING";
            jdbcTemplate.update(sql4, auth.getAuthorId(), followeeId);

            return true;
        }
    }


    @Override
    public UserRecord getById(long userId) {
        String sql1 = """
                select AuthorId, AuthorName, Gender, Age,
                Password, IsDeleted
                from users where authorId = ?;
                """;

        UserRecord userRecord;
        try {
            userRecord = jdbcTemplate.queryForObject(sql1,
                    new BeanPropertyRowMapper<>(UserRecord.class),
                    userId
            );
        } catch (Exception e) {
            throw new SecurityException("user doesn't exist");
        }

        String sql2 = "select FollowerId from user_follows where FollowingId = ?";
        String sql3 = "select FollowingId from user_follows where FollowerId = ?";

        List<Long> followerIds = jdbcTemplate.queryForList(sql2, Long.class, userId);
        List<Long> followingIds = jdbcTemplate.queryForList(sql3, Long.class, userId);

        userRecord.setFollowerUsers(followerIds.stream().mapToLong(Long::longValue).toArray());
        userRecord.setFollowingUsers(followingIds.stream().mapToLong(Long::longValue).toArray());
        userRecord.setFollowing(followingIds.size());
        userRecord.setFollowers(followerIds.size());

        return userRecord;
    }

    @Override
    public void updateProfile(AuthInfo auth, String gender, Integer age) {
        if (gender != null && !gender.equals("Male")
            && !gender.equals("Female") && auth != null) {
            throw new SecurityException("invalid gender");
        }

        String sql1 = "update users set gender = ?, age = ? where authorId = ?";
        String sql2 = "update users set age = ? where authorId = ?";
        String sql3 = "update users set gender = ? where authorId = ?";

        try {
            if (gender == null && age != null) {
                jdbcTemplate.update(sql2, age, auth.getAuthorId());
            }

            if (age == null && gender != null) {
                jdbcTemplate.update(sql3, gender, auth.getAuthorId());
            }

            if (age != null && gender != null) {
                jdbcTemplate.update(sql1, gender, age, auth.getAuthorId());
            }

        } catch (Exception e) {
            throw new SecurityException("not find authorId");
        }

    }

    @Override
    public PageResult<FeedItem> feed(AuthInfo auth, int page, int size, String category) {
        return null;
    }

    @Override
    public Map<String, Object> getUserWithHighestFollowRatio() {
        return null;
    }

}