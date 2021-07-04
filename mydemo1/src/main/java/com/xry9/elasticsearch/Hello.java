package com.xry9.elasticsearch;

public class Hello {
    public static void main(String[] args) {
//        char c = '龥';// 一 4E00, 龥 9FA5
//        Integer i = (int) c;
//        int i1 = 0x9FA5;
//        System.out.println(i);
//        System.out.println(i1);
        Hello hello = new Hello();
        hello.f1();
    }
    public void f1(){
        new H1().ff();
    }
    class H1{
        public void ff(){
            System.out.println(this.getClass().getName());
            System.out.println(Hello.this.getClass().getName());
        }
    }
}
