package org.voltdb.xdcrutil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

/**
 * Utility class containing methods for working with XDCR conflcit records.
 *
 */
public class XdcrUtils {

	/**
	 * @param rowType String from XDCR conflict report
	 * @return Matching ENUM
	 * @throws XdcrFormatException
	 */
	public static XdcrRowType getRowType(String rowType) throws XdcrFormatException  {
		
		if (rowType.replace("\"", "").equals("EXT")) {
			return XdcrRowType.EXT;
		} else if (rowType.replace("\"", "").equals("EXP")) {
			return XdcrRowType.EXP;
		}else if (rowType.replace("\"", "").equals("NEW")) {
			return XdcrRowType.NEW;
		}else if (rowType.replace("\"", "").equals("DEL")) {
			return XdcrRowType.DEL;
		}
		
		throw new XdcrFormatException("Unrecognized rowType: " + rowType);
		
	}

	/**
	 * @param actionType String from XDCR conflict report
	 * @return Matching ENUM
	 * @throws XdcrFormatException
	 */
	public static XdcrActionType getActionType(String actionType)  throws XdcrFormatException {
		
		
		if (actionType.replace("\"", "").equals("I")) {
			return XdcrActionType.I;
		} else if (actionType.replace("\"", "").equals("U")) {
			return XdcrActionType.U;
		}else if (actionType.replace("\"", "").equals("D")) {
			return XdcrActionType.D;
		}
		
		
		throw new XdcrFormatException("Unrecognized XdcrActionType: " + actionType);
	}

	/**
	 * @param conflictType String from XDCR conflict report
	 * @return Matching ENUM
	 * @throws XdcrFormatException
	 */
	public static XdcrConflictType getConflictType(String conflictType)  throws XdcrFormatException {
		
		if (conflictType.replace("\"", "").equals("MISS")) {
			return XdcrConflictType.MISS;
		} else if (conflictType.replace("\"", "").equals("MSMT")) {
			return XdcrConflictType.MSMT;
		}else if (conflictType.replace("\"", "").equals("CNST")) {
			return XdcrConflictType.CNST;
		}else if (conflictType.replace("\"", "").equals("NONE")) {
			return XdcrConflictType.NONE;
		}
		throw new XdcrFormatException("Unrecognized XdcrConflictType: " + conflictType);
	}
	
	/**
	 * Turn true into 1, false into 0.
	 * @param inputValue
	 * @return 1 or 0.
	 */
	public static int mapBooleanToInt(boolean inputValue) {
		
		if (inputValue) {
			return 1;
		}

		return 0;
	}
	
	public static HashMap<String,List<String>> getPKs(Client c) throws NoConnectionsException, IOException, ProcCallException {
	   
	    HashMap<String,List<String>> pkMap = new HashMap<String,List<String>>();
	    
	    ClientResponse cr = c.callProcedure("@SystemCatalog", "PRIMARYKEYS");
	    
	    while (cr.getResults()[0].advanceRow()) {
            String tableName = cr.getResults()[0].getString("TABLE_NAME");
            String columnName = cr.getResults()[0].getString("COLUMN_NAME");
            //Slong   columnSeq = cr.getResults()[0].getLong("KEY_SEQ");
            
            List<String> existingList = pkMap.get(tableName);
            
            if (existingList == null) {
                existingList = new ArrayList<String>();
                existingList.add(columnName);
                pkMap.put(tableName, existingList);
            } else {
                existingList.add(columnName);
            }
            
	    }
	    
	    return pkMap;
	    
	}
	
	public static String getPk(XdcrConflictMessage message, HashMap<String,List<String>> pks) {
	    
	    
	    if (message.getJsonEncodedTuple() == null) {
	        return null;
	    }
	    
	    StringBuffer pk = new StringBuffer();
	    List<String> ourPkFields = pks.get(message.getTableName());
	    
	    //return m_JsonEncodedTuple.split(",")[3].split("\"")[3];
	    
	    for (int i = 0; i < ourPkFields.size(); i++) {
	        
	        StringBuffer b = new StringBuffer('\"');
	        b.append(ourPkFields.get(i));
	        b.append("\":\"");
	        final String pkFieldName = b.toString();
            int indexofPKfield = message.getJsonEncodedTuple().toLowerCase().indexOf(pkFieldName.toLowerCase());
            int nextDoubleQuote = message.getJsonEncodedTuple().indexOf('\"',indexofPKfield + pkFieldName.length() );
            pk.append('\t');
            pk.append(message.getJsonEncodedTuple().substring(indexofPKfield + pkFieldName.length(),nextDoubleQuote ));
            
	            
	       
	    }
	    
	    
	    
	    return pk.toString();
	    
	}


}
