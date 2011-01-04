package cassandra.mapper.engine.annotation;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import cassandra.mapper.api.Transformer;
import cassandra.mapper.api.annotation.Transformed;
import cassandra.mapper.api.exception.CassandraEngineException;
import cassandra.mapper.engine.utils.ReflectionUtils;
import cassandra.mapper.transformer.DateTransformer;
import cassandra.mapper.transformer.DoubleTransformer;
import cassandra.mapper.transformer.FloatTransformer;
import cassandra.mapper.transformer.IntegerTransformer;
import cassandra.mapper.transformer.LongTransformer;
import cassandra.mapper.transformer.ShortTransformer;
import cassandra.mapper.transformer.StringTransformer;
import cassandra.mapper.transformer.UUIDTransformer;

public class TransformedAnnotationProcessor {

	private Map<Field, Transformer> transformerInformation;
	private static final Map<Class<?>, Transformer> knownTransformers = new HashMap<Class<?>, Transformer>();

	public TransformedAnnotationProcessor(Class<?> clazz) {

		initKnownTransformers();
		
		transformerInformation = new HashMap<Field, Transformer>();
		for (Field field : ReflectionUtils.getAnnotatedFields(clazz, Transformed.class)) {
			transformerInformation.put(field, getFieldTransformer(field));
		}
	}
	
	private void initKnownTransformers() {
		
		knownTransformers.put(Short.TYPE, new ShortTransformer());
		knownTransformers.put(Integer.TYPE, new IntegerTransformer());
		knownTransformers.put(Long.TYPE, new LongTransformer());
		knownTransformers.put(Float.TYPE, new FloatTransformer());
		knownTransformers.put(Double.TYPE, new DoubleTransformer());
		knownTransformers.put(String.class, new StringTransformer());
		knownTransformers.put(UUID.class, new UUIDTransformer());
		knownTransformers.put(Date.class, new DateTransformer());
	}
	
	public Transformer getColumnTransformer(Field field) {

		Transformer transformer = transformerInformation.get(field);
		
		if(transformer == null) {
			
			transformer = knownTransformers.get(field.getType());
		}
		
		if(transformer == null) {
			
			throw new CassandraEngineException(String.format("No transformer found for field %s", field.getName()));
		}
		
		return transformer;
	}

	private Transformer getFieldTransformer(Field field) {

		try {
			Transformed annotation = field.getAnnotation(Transformed.class);
			return annotation == null ? null : annotation.transformer().newInstance();
		} catch (Exception ex) {
			throw new CassandraEngineException(ex);
		}
	}

}
