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



    @Override
    public long register(RegisterUserReq req) {
      return 0;
    }

    @Override
    public long login(AuthInfo auth) {
       return 0;
    }

    @Override
    public boolean deleteAccount(AuthInfo auth, long userId) {
        return false;
    }

    @Override
    public boolean follow(AuthInfo auth, long followeeId) {
     return false;
    }


    @Override
    public UserRecord getById(long userId) {
        return null;
    }

    @Override
    public void updateProfile(AuthInfo auth, String gender, Integer age) {

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