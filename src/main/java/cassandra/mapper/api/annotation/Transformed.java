package cassandra.mapper.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import cassandra.mapper.api.Transformer;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Transformed {

	Class<? extends Transformer> transformer();

}
