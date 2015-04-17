package com.yy.cs.dbutil.aop;

import java.lang.reflect.Method;

import org.springframework.aop.AfterReturningAdvice;
import org.springframework.aop.MethodBeforeAdvice;
import org.springframework.aop.ThrowsAdvice;

import com.yy.cs.dbutil.datasource.DynamicDataSource;

public class DataSourceAdvisor implements MethodBeforeAdvice, AfterReturningAdvice, ThrowsAdvice {

    public void afterThrowing(Method method, Object[] args, Object target, Exception ex) {
        DataSourceChange annotation = method.getAnnotation(DataSourceChange.class);
        if (null != annotation.value() && !"".equals(annotation.value())) {
            DynamicDataSource.reset();
        }
    }

    @Override
    public void afterReturning(Object returnValue, Method method, Object[] args, Object target) throws Throwable {
        DataSourceChange annotation = method.getAnnotation(DataSourceChange.class);
        if (null != annotation.value() && !"".equals(annotation.value())) {
            DynamicDataSource.reset();
        }
    }

    @Override
    public void before(Method method, Object[] args, Object target) throws Throwable {
        DataSourceChange annotation = method.getAnnotation(DataSourceChange.class);
        if (null != annotation.value() && !"".equals(annotation.value())) {
            DynamicDataSource.use(annotation.value());
        }
    }

}
