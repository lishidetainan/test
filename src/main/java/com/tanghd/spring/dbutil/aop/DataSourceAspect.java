package com.yy.cs.dbutil.aop;

import java.lang.reflect.Method;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;

import com.yy.cs.dbutil.datasource.DynamicDataSource;

@Aspect
public class DataSourceAspect {

    @Pointcut(value = "@annotation(com.yy.cs.dbutil.aop.DataSourceChange)")
    private void changeDS() {
    }

    @Around(value = "changeDS() ", argNames = "pjp")
    public Object doAround(ProceedingJoinPoint pjp) throws Throwable {
        Object retVal = null;
        MethodSignature ms = (MethodSignature) pjp.getSignature();
        Method method = ms.getMethod();
        DataSourceChange annotation = method.getAnnotation(DataSourceChange.class);
        boolean selectedDataSource = false;
        try {
            String selDs = null;
            if (null != annotation.value() && !"".equals(annotation.value())) {
                selectedDataSource = true;
                selDs = annotation.value();
                DynamicDataSource.use(selDs);
            }
            retVal = pjp.proceed();
        } catch (Throwable e) {
            throw e;
        } finally {
            if (selectedDataSource) {
                DynamicDataSource.reset();
            }
        }
        return retVal;
    }
}
