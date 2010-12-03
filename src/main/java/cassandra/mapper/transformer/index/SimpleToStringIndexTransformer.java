package cassandra.mapper.transformer.index;

import cassandra.mapper.api.IndexTransformer;

public class SimpleToStringIndexTransformer implements IndexTransformer {

	@Override
	public String toIndexKey(Object object) {
		
		return object.toString();
	}

}
