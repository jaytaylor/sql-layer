package com.akiban.cserver.store;

import java.util.ArrayList;
import java.util.List;

import com.akiban.cserver.RowData;

public class TableStatistics {

	private final int rowDefId;
	
	private long rowCount;
	
	private long autoIncrementValue;
	
	private int meanRecordLength;
	
	private int blockSize;
	
	private long creationTime;
	
	private long updateTime;
	
	private List<Histogram> histograms = new ArrayList<Histogram>();
	
	public TableStatistics(final int rowDefId) {
		this.rowDefId = rowDefId;
	}
	
	public long getRowCount() {
		return rowCount;
	}

	public void setRowCount(long rowCount) {
		this.rowCount = rowCount;
	}

	public long getAutoIncrementValue() {
		return autoIncrementValue;
	}

	public void setAutoIncrementValue(long autoIncrementValue) {
		this.autoIncrementValue = autoIncrementValue;
	}

	public int getMeanRecordLength() {
		return meanRecordLength;
	}

	public void setMeanRecordLength(int meanRecordLength) {
		this.meanRecordLength = meanRecordLength;
	}

	public int getBlockSize() {
		return blockSize;
	}

	public void setBlockSize(int blockSize) {
		this.blockSize = blockSize;
	}

	public long getCreationTime() {
		return creationTime;
	}

	public void setCreationTime(long creationTime) {
		this.creationTime = creationTime;
	}

	public long getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(long updateTime) {
		this.updateTime = updateTime;
	}

	public List<Histogram> getHistogramList() {
		return histograms;
	}

	public void addHistogram(Histogram histogram) {
		this.histograms.add(histogram);
	}

	public int getRowDefId() {
		return rowDefId;
	}
	
	public class Histogram {
		
		private final int indexId;
		
		private List<HistogramSample> samples = new ArrayList<HistogramSample>();
		
		Histogram(final int indexId) {
			this.indexId = indexId;
		
		}

		public int getIndexId() {
			return indexId;
		}
		
		public final List<HistogramSample> getHistogramSamples() {
			return samples;
		}
		
		public void addSample(final HistogramSample sample) {
			this.samples.add(sample);
		}
	}
	
	public class HistogramSample {
		
		HistogramSample(final RowData rowData, final long rowCount) {
			this.rowData = rowData;
			this.rowCount = rowCount;
		}
		
		private final RowData rowData;
		
		public RowData getRowData() {
			return rowData;
		}

		public long getRowCount() {
			return rowCount;
		}
		
		private final long rowCount;
		
	}
}
