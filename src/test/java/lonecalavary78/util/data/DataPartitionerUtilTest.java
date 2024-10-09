package lonecalavary78.util.data;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.stream.IntStream;

class DataPartitionerUtilTest {
  @Test
  @DisplayName("Test with empty data set")
  void testWithEmptyDataSet() {
    var dataSet = new ArrayList<String>();
    var dataPartitionerUtil = new DataPartitionerUtil<String>();
    Assertions.assertThrows(IllegalArgumentException.class,()->dataPartitionerUtil.splitIntoPartitioner(dataSet));
  }

  @Test
  @DisplayName("Test with the filled data sets")
  void withFilledDataSet() {
    var dataSet = IntStream.range(0,5).mapToObj(counter->"product-%s".formatted(counter+1)).toList();
    var dataPartitionerUtil = new DataPartitionerUtil<String>();
    Assertions.assertAll(
            ()->Assertions.assertDoesNotThrow(()->dataPartitionerUtil.splitIntoPartitioner(dataSet)),
            ()->Assertions.assertNotNull(dataPartitionerUtil.splitIntoPartitioner(dataSet))
    );
  }

  @ParameterizedTest
  @DisplayName("Test with the smaller data sets")
  @ValueSource(ints = {10, 20})
  void withSmallerDataSets(int numberOfDataSets) {
    var dataSet = IntStream.range(0,numberOfDataSets).mapToObj(counter->"product-%s".formatted(counter+1)).toList();
    var dataPartitionerUtil = new DataPartitionerUtil<String>();
    var partitioners = dataPartitionerUtil.splitIntoPartitioner(dataSet);
    var expectedTotalPartition = Math.ceilDiv(numberOfDataSets,10);
    Assertions.assertEquals(expectedTotalPartition, partitioners.size());
  }

  @ParameterizedTest
  @DisplayName("Test with larger data sets")
  @ValueSource(ints = {9999, 100000, 1000030})
  void withLargerDataSets(int numberOfDataSets) {
    var dataSet = IntStream.range(0,numberOfDataSets).mapToObj(counter->"product-%s".formatted(counter+1)).toList();
    var dataPartitionerUtil = new DataPartitionerUtil<String>();
    var partitioners = dataPartitionerUtil.splitIntoPartitioner(dataSet);
    var maximumRecordsPerChunk = getMaximumRecordsPerChunks(dataSet.size());
    var expectedTotalPartition = Math.ceilDiv(dataSet.size(),maximumRecordsPerChunk);
    Assertions.assertEquals(expectedTotalPartition, partitioners.size());
    partitioners.stream().forEach(partitioner->Assertions.assertEquals(maximumRecordsPerChunk,partitioner.partitionedDataSet().size()));
  }

  private int getMaximumRecordsPerChunks(int totalRecords) {
    var baseNumber = 10;
    var exponent = Math.ceil(Math.log10(totalRecords));
    if(exponent<=2 && exponent>1)
      exponent-=1;
    else if(exponent>2)
      exponent-=2;
    return (int)Math.pow(baseNumber, exponent);
  }
}
