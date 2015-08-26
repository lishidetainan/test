package com.tanghd.spring.dbutil.aop;

import java.lang.reflect.Method;

import org.springframework.aop.AfterReturningAdvice;
import org.springframework.aop.MethodBeforeAdvice;
import org.springframework.aop.ThrowsAdvice;

import com.tanghd.spring.dbutil.datasource.DynamicDataSource;

public class SwitchSlaveDataSourceAdvisor implements MethodBeforeAdvice, AfterReturningAdvice, ThrowsAdvice {

    private static final ThreadLocal<Boolean> switched = new ThreadLocal<Boolean>() {

        @Override
        protected Boolean initialValue() {
            return false;
        }

    };

    public void afterThrowing(Method method, Object[] args, Object target, Exception ex) {
        if (switched.get()) {
            DynamicDataSource.reset();
        }
    }

    @Override
    public void afterReturning(Object returnValue, Method method, Object[] args, Object target) throws Throwable {
        if (switched.get()) {
            DynamicDataSource.reset();
        }
    }

    @Override
    public void before(Method method, Object[] args, Object target) throws Throwable {
        DynamicDataSource.useSlave();
        switched.set(true);
    }

}
