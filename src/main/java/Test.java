public class Test {
    static class A{
        public A(){
            System.out.println("a");
        }
        public A(String a){
            System.out.println("A");
        }
    }
    static class B extends A{
        public B(){
            //super("a");
        }
    }
    public static void main(String[] args) {
        B b=new B();
    }
}
