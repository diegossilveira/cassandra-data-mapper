package cassandra.mapper.engine.annotation;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import cassandra.mapper.api.Transformer;
import cassandra.mapper.api.annotation.Transformed;
import cassandra.mapper.api.exception.CassandraEngineException;
import cassandra.mapper.engine.utils.ReflectionUtils;
import cassandra.mapper.transformer.TransformerFactory;

public class TransformedAnnotationProcessor {

	private Map<Field, Transformer> transformerInformation;

	public TransformedAnnotationProcessor(Class<?> clazz) {

		transformerInformation = new HashMap<Field, Transformer>();
		for (Field field : ReflectionUtils.getAnnotatedFields(clazz, Transformed.class)) {
			transformerInformation.put(field, getFieldTransformer(field));
		}
	}

	public Transformer getColumnTransformer(Field field) {

		Transformer transformer = transformerInformation.get(field);

		if (transformer == null) {

			transformer = TransformerFactory.forClass(field.getType());
		}

		if (transformer == null) {

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
