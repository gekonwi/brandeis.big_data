package hadoop08.utils;

public class Test {

	
	
	public static void main(String[] args) {
		StringInteger test = new StringInteger("HIHIHI", 10);
		System.out.println(test.getString());
		System.out.println(test.getValue());
		System.out.println(test.toString());
		
		StringDouble test2 = new StringDouble("BYEBYEBYE", .11111);
		System.out.println(test2.getString());
		System.out.println(test2.getValue());
		System.out.println(test2.toString());
	}

}
