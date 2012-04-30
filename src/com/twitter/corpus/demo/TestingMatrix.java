package com.twitter.corpus.demo;

public class TestingMatrix {
	public TestingMatrix(){}
	public static void main(){

		TestingMatrix tm = new TestingMatrix();
		boolean[][] termMtrx = new boolean[][]{{true,true,true,false,false},{true,false,true,false,false},{true,true,false,true,true},{false,false,false,true,false},{false,false,false,true,true}};
		double[][] coWeight = new double[termMtrx.length][termMtrx.length];

		//**********************//
		// Co-Weight Matrix
		//**********************//
		for(int i=0 ; i<termMtrx.length ; i++){
			for(int j=0 ; j<termMtrx.length  ; j++){
				coWeight[i][j] = tm.mult(termMtrx[i], termMtrx[j]);
			}
		}

		//			BufferedWriter out = new BufferedWriter(new FileWriter("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/fizbuz"));
		//			for(int i=0;i<cluster[0].length;i++){
		//				StringBuffer sb = new StringBuffer();
		//				for(int j=0;j<cluster[0].length;j++){
		//					sb.append(Integer.toString(cluster[i][j]) + ",");
		//				}
		//				out.write(sb.toString() + "\n");
		//			}
		//			out.close();

		// compute Matrix of Jaccard's Coefficients

		double[][] jMatrix = new double[termMtrx.length][termMtrx.length];

		for(int i=0 ; i < termMtrx.length ;i++){
			for(int j=0; j<termMtrx.length;j++){
				if(coWeight[i][j]!=0){
					jMatrix[i][j] = coWeight[i][j] / (coWeight[i][i] + coWeight[j][j] - coWeight[i][j]);
				}
			}
		}
	}
	private double mult(boolean[] a, boolean[] b){
		double retVal = 0.0;
		//		short retVal = 0;
		for(int i=0; i< a.length ;i++){
			if(a[i]==true && b[i]==true){
				retVal+=10.0;}
		}
		return retVal;
	}
}