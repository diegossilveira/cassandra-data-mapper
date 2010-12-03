package cassandra.mapper.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import cassandra.mapper.api.IndexTransformer;
import cassandra.mapper.transformer.index.SimpleToStringIndexTransformer;


@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Index {

	String name();
	String columnFamily();
	Class<? extends IndexTransformer> transformer() default SimpleToStringIndexTransformer.class;
	
}
