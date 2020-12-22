package com.ngw;

public class Constant {
    public enum Method{
        POST("POST"),GET("GET"),PUT("PUT"),DELETE("DELETE"),HEAD("HEAD");
        private String methodValue;

        private Method(String methodValue){
            this.methodValue = methodValue;
        }

        public String getString(){
            return methodValue;
        }
    }
}
