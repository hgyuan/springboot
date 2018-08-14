package com.demo.springboot.transform;

import com.demo.springboot.mybatis.model.SUserDomain;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class ESTransformer {

    public static Map<String,Object> transform2Map(Object object) {
        Map<String,Object> retMap = new HashMap<String,Object>();
        Class clazz = object.getClass();
        Field[]  fields  = object.getClass().getDeclaredFields();
        for(Field field : fields){
            try {
                PropertyDescriptor propertyDescriptor = new PropertyDescriptor(field.getName(),clazz);
                try {
                    retMap.put(field.getName(),propertyDescriptor.getReadMethod().invoke(object));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            } catch (IntrospectionException e) {
                e.printStackTrace();
            }

        }



        return retMap;
    }

    public static void main (String[] args){
        SUserDomain vo = new SUserDomain();
        vo.setEmail("email@email.com");
        vo.setId(1);
        vo.setEnabled("2");
        vo.setUserName("admin");
        try {
            transform2Map(vo);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
