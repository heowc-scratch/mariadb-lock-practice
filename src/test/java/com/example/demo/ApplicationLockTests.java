package com.example.demo;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ApplicationLockTests {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final String APPLICATION_LOCK = "select GET_LOCK(?, 10)";

    private static final String APPLICATION_UNLOCK = "select RELEASE_LOCK(?)";

    private static final String IS_APPLICATION_LOCKED = "select IS_FREE_LOCK(?)";

    private static final String COUNT_ALL_SQL = "SELECT COUNT(*) FROM push_message";

    private static final String INSERT_SQL = "INSERT INTO push_message (`content`, `status`, `created_by`, `created_at`, `last_modified_at`) VALUES (?, ?, ?, ?, ?)";

    private static final String LOCK_PREFIX_NAME = "lock_%s";
    private static final String TABLE_NAME = "push_message";

    private static final String TRUNCATE_SQL = "TRUNCATE push_message";

    private boolean isUnlocked() {
        return Objects.equals(jdbcTemplate.queryForObject(IS_APPLICATION_LOCKED, Integer.class, String.format(LOCK_PREFIX_NAME, TABLE_NAME)), 1);
    }

    @Test
    public void 애플리케이션_락_걸고_입력시_입력이_안되어_데이터_갯수_조회시_0_반환() {
        // given
        List<PushMessage> pushMessageList = Arrays.asList(
                new PushMessage("content 1", "wait", "heowc", LocalDateTime.now(Clock.systemUTC()), null),
                new PushMessage("content 2", "wait", "heowc", LocalDateTime.now(Clock.systemUTC()), null));

        // when
        jdbcTemplate.queryForMap(APPLICATION_LOCK, String.format(LOCK_PREFIX_NAME, TABLE_NAME));

        if (isUnlocked()) {
            for (PushMessage pushMessage : pushMessageList) {
                jdbcTemplate.update(INSERT_SQL, pushMessage.getContent(), pushMessage.getStatus(), pushMessage.getCreatedBy(), pushMessage.getCreatedAt(), pushMessage.getLastModifiedAt());
            }
        }

        jdbcTemplate.queryForMap(APPLICATION_UNLOCK, String.format(LOCK_PREFIX_NAME, TABLE_NAME));

        // then
        Long count = jdbcTemplate.queryForObject(COUNT_ALL_SQL, Long.class);
        assertThat(count).isEqualTo(0L);
    }

    @Test
    public void 입력시_데이터_갯수_조회시_2_반환() {
        // given
        List<PushMessage> pushMessageList = Arrays.asList(
                new PushMessage("content 1", "wait", "heowc", LocalDateTime.now(Clock.systemUTC()), null),
                new PushMessage("content 2", "wait", "heowc", LocalDateTime.now(Clock.systemUTC()), null));

        // when
        if (isUnlocked()) {
            for (PushMessage pushMessage : pushMessageList) {
                jdbcTemplate.update(INSERT_SQL, pushMessage.getContent(), pushMessage.getStatus(), pushMessage.getCreatedBy(), pushMessage.getCreatedAt(), pushMessage.getLastModifiedAt());
            }
        }

        // then
        Long count = jdbcTemplate.queryForObject(COUNT_ALL_SQL, Long.class);
        assertThat(count).isEqualTo(2L);
    }


    @After
    public void delete() {
        jdbcTemplate.update(TRUNCATE_SQL);
    }
}
