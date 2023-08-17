package com.cq.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class MyLength extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1) {
            throw new UDFArgumentException("只接受一个参数");
        }
        ObjectInspector argument = objectInspectors[0];
        if (ObjectInspector.Category.PRIMITIVE != argument.getCategory()) {
            throw new UDFArgumentException("只接受基本数据类型");
        }

        PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) argument;
        if (primitiveObjectInspector.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("只接受String");
        }

        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;

    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        DeferredObject argument = arguments[0];
        Object o = argument.get();
        if (o == null) {
            return 0;
        }
        return o.toString().length();
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
