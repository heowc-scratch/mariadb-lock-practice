package com.example.demo;

import static org.assertj.core.api.Assertions.*;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class GlobalLockTests {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	private static final String GLOBAL_READ_LOCK = "FLUSH TABLES WITH READ LOCK";

	private static final String TABLE_UNLOCK = "UNLOCK TABLES";

	private static final String SELECT_BY_ID_SQL = "SELECT `id`, `content`, `status`, `created_by`, `created_at`, `last_modified_at` FROM push_message WHERE status = ? LIMIT 1";

	private static final String INSERT_SQL = "INSERT INTO push_message (`content`, `status`, `created_by`, `created_at`, `last_modified_at`) VALUES (?, ?, ?, ?, ?)";

	private static final String TRUNCATE_SQL = "TRUNCATE push_message";

	@Test
	public void 데이터_입력_후_조회시_성공() {
		// given
		List<PushMessage> pushMessageList = Arrays.asList(
				new PushMessage("content 1", "wait", "heowc", LocalDateTime.now(Clock.systemUTC()), null),
				new PushMessage("content 2", "wait", "heowc", LocalDateTime.now(Clock.systemUTC()), null));

		// when
		for (PushMessage pushMessage : pushMessageList) {
			jdbcTemplate.update(INSERT_SQL, pushMessage.getContent(), pushMessage.getStatus(), pushMessage.getCreatedBy(), pushMessage.getCreatedAt(), pushMessage.getLastModifiedAt());
		}

		// then
		Map<String, Object> result = jdbcTemplate.queryForMap(SELECT_BY_ID_SQL, "wait");
		assertThat(result.get("content")).isEqualTo("content 1");
		assertThat(result.get("status")).isEqualTo("wait");
		assertThat(result.get("created_by")).isEqualTo("heowc");
	}

	@Test
	public void 글로벌_read_락_걸고_입력시_실패() {
		// given
		List<PushMessage> pushMessageList = Arrays.asList(
				new PushMessage("content 1", "wait", "heowc", LocalDateTime.now(Clock.systemUTC()), null),
				new PushMessage("content 2", "wait", "heowc", LocalDateTime.now(Clock.systemUTC()), null));

		jdbcTemplate.execute(GLOBAL_READ_LOCK);

		// when-then
		assertThatThrownBy(() -> {
			for (PushMessage pushMessage : pushMessageList) {
				jdbcTemplate.update(INSERT_SQL, pushMessage.getContent(), pushMessage.getStatus(), pushMessage.getCreatedBy(), pushMessage.getCreatedAt(), pushMessage.getLastModifiedAt());
			}
		}).isInstanceOf(UncategorizedSQLException.class);
		// Caused by: java.sql.SQLException: (conn=227) Table 'push_message' was locked with a READ lock and can't be updated

		jdbcTemplate.execute(TABLE_UNLOCK);
	}

	@Test
	public void 데이터_입력_후_글로벌_read_락_걸고_조회시_성공() {
		// given
		List<PushMessage> pushMessageList = Arrays.asList(
				new PushMessage("content 1", "wait", "heowc", LocalDateTime.now(Clock.systemUTC()), null),
				new PushMessage("content 2", "wait", "heowc", LocalDateTime.now(Clock.systemUTC()), null));

		// when
		for (PushMessage pushMessage : pushMessageList) {
			jdbcTemplate.update(INSERT_SQL, pushMessage.getContent(), pushMessage.getStatus(), pushMessage.getCreatedBy(), pushMessage.getCreatedAt(), pushMessage.getLastModifiedAt());
		}
		jdbcTemplate.execute(GLOBAL_READ_LOCK);

		// then
		Map<String, Object> result = jdbcTemplate.queryForMap(SELECT_BY_ID_SQL, "wait");
		assertThat(result.get("content")).isEqualTo("content 1");
		assertThat(result.get("status")).isEqualTo("wait");
		assertThat(result.get("created_by")).isEqualTo("heowc");

		jdbcTemplate.execute(TABLE_UNLOCK);
	}

	@After
	public void delete() {
		jdbcTemplate.update(TRUNCATE_SQL);
	}

}
