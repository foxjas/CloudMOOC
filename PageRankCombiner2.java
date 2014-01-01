/*
 * Software License, Version 1.0
 *
 *  Copyright 2003 The Trustees of Indiana University.  All rights reserved.
 *
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1) All redistributions of source code must retain the above copyright notice,
 *  the list of authors in the original source code, this list of conditions and
 *  the disclaimer listed in this license;
 * 2) All redistributions in binary form must reproduce the above copyright
 *  notice, this list of conditions and the disclaimer listed in this license in
 *  the documentation and/or other materials provided with the distribution;
 * 3) Any documentation included with all redistributions must include the
 *  following acknowledgement:
 *
 * "This product includes software developed by the Community Grids Lab. For
 *  further information contact the Community Grids Lab at
 *  http://communitygrids.iu.edu/."
 *
 *  Alternatively, this acknowledgement may appear in the software itself, and
 *  wherever such third-party acknowledgments normally appear.
 *
 * 4) The name Indiana University or Community Grids Lab or Twister,
 *  shall not be used to endorse or promote products derived from this software
 *  without prior written permission from Indiana University.  For written
 *  permission, please contact the Advanced Research and Technology Institute
 *  ("ARTI") at 351 West 10th Street, Indianapolis, Indiana 46202.
 * 5) Products derived from this software may not be called Twister,
 *  nor may Indiana University or Community Grids Lab or Twister appear
 *  in their name, without prior written permission of ARTI.
 *
 *
 *  Indiana University provides no reassurances that the source code provided
 *  does not infringe the patent or any other intellectual property rights of
 *  any other entity.  Indiana University disclaims any liability to any
 *  recipient for claims brought by any other entity based on infringement of
 *  intellectual property rights or otherwise.
 *
 * LICENSEE UNDERSTANDS THAT SOFTWARE IS PROVIDED "AS IS" FOR WHICH NO
 * WARRANTIES AS TO CAPABILITIES OR ACCURACY ARE MADE. INDIANA UNIVERSITY GIVES
 * NO WARRANTIES AND MAKES NO REPRESENTATION THAT SOFTWARE IS FREE OF
 * INFRINGEMENT OF THIRD PARTY PATENT, COPYRIGHT, OR OTHER PROPRIETARY RIGHTS.
 * INDIANA UNIVERSITY MAKES NO WARRANTIES THAT SOFTWARE IS FREE FROM "BUGS",
 * "VIRUSES", "TROJAN HORSES", "TRAP DOORS", "WORMS", OR OTHER HARMFUL CODE.
 * LICENSEE ASSUMES THE ENTIRE RISK AS TO THE PERFORMANCE OF SOFTWARE AND/OR
 * ASSOCIATED MATERIALS, AND TO THE PERFORMANCE AND VALIDITY OF INFORMATION
 * GENERATED USING SOFTWARE.
 */

package cgl.imr.samples.pagerank;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import cgl.imr.base.Combiner;
import cgl.imr.base.Key;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.types.BytesValue;
import cgl.imr.types.DoubleVectorData;

/**
 * Combine the partial result of page result together.
 * 
 * @author Hui Li (lihui@indiana.edu)
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 * 
 */

public class PageRankCombiner2 implements Combiner {

	DoubleVectorData results;

	public PageRankCombiner2() {
		results = new DoubleVectorData();
	}

	public void close() throws TwisterException {
	}

	public void combine(Map<Key, Value> keyValues) throws TwisterException {
		try {
			
			int numUrls = -1;	        
			double[][] newPageRanks = new double[numUrls][1];
			double[][] currPageRanks; 
			double totalDanglingValSum = 0.0;
			
			/* Write your code and COMPLETE HERE */
			//shouldn't there only be 1 key? (see end of Reduce) 
            for (Iterator<Key> ite = keyValues.keySet().iterator(); ite.hasNext();) {
            		Key key = ite.next();
                    BytesValue val = (BytesValue) keyValues.get(key);
                    DoubleVectorData tmpDV = new DoubleVectorData();
                    tmpDV.fromBytes(val.getBytes());
                    numUrls = tmpDV.getNumData() - 1;
                    currPageRanks = tmpDV.getData();
                    totalDanglingValSum += currPageRanks[numUrls][0]; // merge dangling values 
                    // merge the changed page rank values together
                    for (int j = 0; j < numUrls; j++) {
                    	newPageRanks[j][0] += currPageRanks[j][0];
                    }
            }			
            // factor in dangling values, damping factor 
            for (int i = 0; i < numUrls; i++) {
            	double pageRankVal = newPageRanks[i][0] + (totalDanglingValSum / numUrls);
            	newPageRanks[i][0] = (.15 / numUrls) + (.85 * pageRankVal);
            }
            
            /** End of your code */ 
			results = new DoubleVectorData(newPageRanks, numUrls, 1); 
			
		} catch (Exception e) {
			throw new TwisterException(e);
		}
	}

	public void configure(JobConf jobConf) throws TwisterException {
	}

	public DoubleVectorData getResults() {
		return results;
	}
}
