package cassandra.mapper.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import cassandra.mapper.api.Transformer;
import cassandra.mapper.transformer.StringTransformer;


@Target({ ElementType.FIELD, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {

	String name() default "";
	Class<? extends Transformer> transformer() default StringTransformer.class;
	
}
