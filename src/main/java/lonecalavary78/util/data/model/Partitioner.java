package lonecalavary78.util.data.model;

import java.util.List;

public record Partitioner<T extends Object>(int index, List<T> partitionedDataSet) {
}
