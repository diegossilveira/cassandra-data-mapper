package cassandra.mapper.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.cassandra.thrift.ConsistencyLevel;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Entity {

	String keyspace();
	String columnFamily();
	ConsistencyLevel writeConsistencyLevel() default ConsistencyLevel.QUORUM;
	ConsistencyLevel readConsistencyLevel() default ConsistencyLevel.ONE;

}
