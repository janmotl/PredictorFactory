package gui;

import controller.MainApp;
import org.junit.Test;

public class MainAppTest {

	@Test
	public void commandLineArguments() {
		String[] args = {"PostgreSQL", "mutagenesis_test_setting"};

		MainApp.main(args);
	}
}
