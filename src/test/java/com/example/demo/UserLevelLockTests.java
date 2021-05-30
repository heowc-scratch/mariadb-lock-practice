package com.example.demo;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class UserLevelLockTests {

    @Autowired
    private UserLevelLock userLevelLock;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final String USER_LOCK = "select GET_LOCK(?, 3)";

    private static final String LOCK_PREFIX_NAME = "lock_%s";
    private static final String TABLE_NAME = "push_message";

    @Test
    void 유저_레발_락_걸고_다른_커낵션에서_유저_레발_락을_얻으려고_할_때_실패() {
        userLevelLock.executeWithLock(String.format(LOCK_PREFIX_NAME, TABLE_NAME), 3, () -> {
            final Integer lockCode =
                    jdbcTemplate.queryForObject(USER_LOCK, Integer.class, String.format(LOCK_PREFIX_NAME, TABLE_NAME));
            assertThat(lockCode).isNotEqualTo(1);
            return Void.TYPE;
        });
    }
}
