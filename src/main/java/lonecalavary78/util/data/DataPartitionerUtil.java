package lonecalavary78.util.data;

import java.util.ArrayList;
import java.util.List;
import lonecalavary78.util.data.model.Partitioner;


public class DataPartitionerUtil<T> {

  public List<Partitioner<T>> splitIntoPartitioner(List<T> dataSet) {
    var partitioners = new ArrayList<Partitioner<T>>();
    if (isDataSetEmpty(dataSet)) {
      throw new IllegalArgumentException("The empty source data is required to perform this operation");
    }
    var maximumRecordPerChunk = getMaximumRecordPerChunk(dataSet.size());
    var totalChunks = Math.ceilDiv(dataSet.size(), maximumRecordPerChunk);
    var counter = 0;
    while (counter < totalChunks) {
      partitioners.add(new Partitioner<>(counter, dataSet.subList(startPos(counter, maximumRecordPerChunk), endPos(counter, maximumRecordPerChunk, dataSet.size()))));
      counter++;
    }
    return partitioners;
  }

  private boolean isDataSetEmpty(List<T> dataSet) {
    return dataSet == null || dataSet.isEmpty();
  }

  private int getMaximumRecordPerChunk(int totalRecords) {
    var baseNumber = 10;
    if (totalRecords <= 0) {
      throw new IllegalArgumentException("The total record with 0 or less is not allow to get calculated divider");
    }

    var exponent = Math.ceil(Math.log10(totalRecords));
    if (exponent <= 2 && exponent > 1) {
      exponent -= 1;
    } else if (exponent > 2) {
      exponent -= 2;
    }
    return (int) Math.pow(baseNumber, exponent);
  }

  private int startPos(int chunkCounter, int maximumRecordPerChunk) {
    return (chunkCounter == 0) ? 0 : (chunkCounter - 1) * maximumRecordPerChunk;
  }

  @SuppressWarnings("checkstyle:ParenPad")
  private int endPos(int chunckCounter, int maximumRecordPerChunk, int totalRecords) {
    var end = (chunckCounter <= 0 ) ? maximumRecordPerChunk
            : chunckCounter * maximumRecordPerChunk;
    return Math.min(end, totalRecords);
  }
}
