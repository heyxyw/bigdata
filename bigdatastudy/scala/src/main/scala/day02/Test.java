package com.zhouq.scala.day02;

/**
 * 可变参数
 *
 */
public class Test {

    public static void main(String[] args) {
        test("123","1212");
    }

    public static void test(String ... age){
        System.out.println(age.length);
    }
}
