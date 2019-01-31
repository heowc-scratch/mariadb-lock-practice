package com.example.demo;

import static org.assertj.core.api.Assertions.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class UserLockTests {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	private static final String USER_LOCK = "select GET_LOCK(?, 3)";

	private static final String USER_UNLOCK = "select RELEASE_LOCK(?)";

	private static final String IS_USER_LOCKED = "select IS_FREE_LOCK(?)";

	private static final String COUNT_ALL_SQL = "SELECT COUNT(*) FROM push_message";

	private static final String INSERT_SQL = "INSERT INTO push_message (`content`, `status`, `created_by`, `created_at`, `last_modified_at`) VALUES (?, ?, ?, ?, ?)";

	private static final String LOCK_PREFIX_NAME = "lock_%s";
	private static final String TABLE_NAME = "push_message";

	private static final String TRUNCATE_SQL = "TRUNCATE push_message";

	private boolean isUnlocked() {
		return Objects.equals(jdbcTemplate.queryForObject(IS_USER_LOCKED, Integer.class, String.format(LOCK_PREFIX_NAME, TABLE_NAME)), 1);
	}

	@Test
	public void 유저_락_걸고_입력시_입력이_안되어_데이터_갯수_조회시_0_반환() {
		// given
		List<PushMessage> pushMessageList = Arrays.asList(
				new PushMessage("content 1", "wait", "heowc", LocalDateTime.now(Clock.systemUTC()), null),
				new PushMessage("content 2", "wait", "heowc", LocalDateTime.now(Clock.systemUTC()), null));

		// when
		jdbcTemplate.queryForMap(USER_LOCK, String.format(LOCK_PREFIX_NAME, TABLE_NAME));

		if (isUnlocked()) {
			for (PushMessage pushMessage : pushMessageList) {
				jdbcTemplate.update(INSERT_SQL, pushMessage.getContent(), pushMessage.getStatus(), pushMessage.getCreatedBy(), pushMessage.getCreatedAt(), pushMessage.getLastModifiedAt());
			}
		}

		jdbcTemplate.queryForMap(USER_UNLOCK, String.format(LOCK_PREFIX_NAME, TABLE_NAME));

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

	@Test
	public void 유저_락을_걸고_커넥션이_안닫힌_상태에서_다시_유저_락을_걸면_0을_반환() throws SQLException {
		// given
		try (Connection connection = jdbcTemplate.getDataSource().getConnection();
				PreparedStatement preparedStatement = connection.prepareStatement(USER_LOCK)) {
			preparedStatement.setString(1, String.format(LOCK_PREFIX_NAME, TABLE_NAME));

			try (ResultSet resultSet = preparedStatement.executeQuery()) {
				resultSet.first();
				final boolean isLocked = resultSet.getInt(1) == 1;
				// when
				// 3초간 sleep
				final boolean result = new Integer(0).equals(jdbcTemplate.queryForObject(USER_LOCK, Integer.class, String.format(LOCK_PREFIX_NAME, TABLE_NAME)));

				// then
				assertThat(isLocked).isTrue();
				assertThat(result).isTrue();
			}
		} catch (SQLException sqlEx) {
			sqlEx.printStackTrace();
		}

		jdbcTemplate.queryForMap(USER_UNLOCK, String.format(LOCK_PREFIX_NAME, TABLE_NAME));
	}

	@After
	public void delete() {
		jdbcTemplate.update(TRUNCATE_SQL);
	}
}
