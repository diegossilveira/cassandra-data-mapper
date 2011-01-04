package cassandra.mapper.transformer;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import cassandra.mapper.api.Transformer;

public final class TransformerFactory {

	private static final Map<Class<?>, Transformer> knownTransformers = new HashMap<Class<?>, Transformer>();
	
	private TransformerFactory() {
	}

	static {

		Transformer transformer = new ShortTransformer();
		knownTransformers.put(Short.TYPE, transformer);
		knownTransformers.put(Short.class, transformer);

		transformer = new IntegerTransformer();
		knownTransformers.put(Integer.TYPE, transformer);
		knownTransformers.put(Integer.class, transformer);

		transformer = new LongTransformer();
		knownTransformers.put(Long.TYPE, transformer);
		knownTransformers.put(Long.class, transformer);

		transformer = new FloatTransformer();
		knownTransformers.put(Float.TYPE, transformer);
		knownTransformers.put(Float.class, transformer);

		transformer = new DoubleTransformer();
		knownTransformers.put(Double.TYPE, transformer);
		knownTransformers.put(Double.class, transformer);

		transformer = new BooleanTransformer();
		knownTransformers.put(Boolean.TYPE, transformer);
		knownTransformers.put(Boolean.class, transformer);

		knownTransformers.put(String.class, new StringTransformer());
		knownTransformers.put(UUID.class, new UUIDTransformer());
		knownTransformers.put(Date.class, new DateTransformer());
	}
	
	public static Transformer forClass(Class<?> clazz) {
		
		return knownTransformers.get(clazz);
	}

}
