package code.lemma;

import java.io.FileNotFoundException;
import java.util.Map;

import org.junit.Test;

public class TokenizerTest {

	@Test
	public void test() throws FileNotFoundException {
		Tokenizer temp = new Tokenizer();
		Map<String, Integer> lemmas = temp
				.tokenize("hi i am so cool and or is we he she -this -is really |good ''asdsdd'' {qweqwe} [qwewqwere] awewe.");
		for (String s : lemmas.keySet())
			System.out.println(s);
	}

}
