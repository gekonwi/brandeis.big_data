package code.lemma;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.util.List;

import org.junit.Test;

public class TokenizerTest {

	@Test
	public void test() throws FileNotFoundException {
		Tokenizer temp = new Tokenizer();
		List<String> lemmas = temp.tokenize("hi i am so cool and or is we he she -this -is really |good ''asdsdd'' {qweqwe} [qwewqwere] awewe.");
		for (String s: lemmas)
			System.out.println(s);
	}

}
