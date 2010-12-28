package cassandra.mapper.engine.serialization;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.apache.log4j.Logger;

import cassandra.mapper.engine.utils.ReflectionUtils;

public class LazyObjectHandler implements MethodInterceptor {

	private final Logger logger = Logger.getLogger(LazyDeserializer.class);
	private final Class<?> clazz;
	private final Map<Field, Object> columnMap;

	public LazyObjectHandler(Class<?> clazz, Map<Field, Object> columnMap) {

		this.clazz = clazz;
		this.columnMap = columnMap;
	}

	@Override
	public Object intercept(Object object, Method method, Object[] args, MethodProxy methodProxy) throws Throwable {

		logger.debug("Proxy: invoked method " + method.getName());

		if (Void.TYPE == method.getGenericReturnType()) {

			return methodProxy.invokeSuper(object, args);
		}

		Field field = ReflectionUtils.getFieldForMethod(clazz, method);

		if (field == null) {

			return methodProxy.invokeSuper(object, args);
		}

		Object value = columnMap.get(field);
		ReflectionUtils.setFieldValue(field, object, value);
		
		return value;
	}
}
