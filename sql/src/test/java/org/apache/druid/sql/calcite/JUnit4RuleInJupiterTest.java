package org.apache.druid.sql.calcite;

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.util.List;

@EnableRuleMigrationSupport
class JUnit4RuleInJupiterTest {

	@Rule
	public ExpectedException thrown =
		ExpectedException.none();

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

	@Test
	void useExpectedExceptionRule() {
		List<Object> list = List.of();
		thrown.expect(
			IndexOutOfBoundsException.class);
		list.get(0);
	}

  @Test
  public void asd() throws Exception
  {
    System.out.println(folder.newFolder());

  }

}