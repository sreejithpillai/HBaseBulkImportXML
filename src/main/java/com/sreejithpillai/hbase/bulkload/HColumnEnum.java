package com.sreejithpillai.hbase.bulkload;

public enum HColumnEnum {
	  COL_AUTHOR ("author".getBytes()),
	  COL_TITLE ("title".getBytes()),
	  COL_GENRE ("genre".getBytes()),
	  COL_PRICE ("price".getBytes()),
	  COL_PUBLISHDATE ("publish_date".getBytes()),
	  COL_DESCRIPTION ("description".getBytes());
	 
	  private final byte[] columnName;
	  
	  HColumnEnum (byte[] column) {
	    this.columnName = column;
	  }

	  public byte[] getColumnName() {
	    return this.columnName;
	  }
	}