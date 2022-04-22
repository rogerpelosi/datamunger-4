package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

import static java.lang.Integer.parseInt;

public class CsvQueryProcessor extends QueryProcessingEngine {

	/*
	 * Parameterized constructor to initialize filename. As you are trying to
	 * perform file reading, hence you need to be ready to handle the IO Exceptions.
	 */
	String fileName;
	public CsvQueryProcessor(String fileName) throws FileNotFoundException {
		FileReader fr = new FileReader(fileName);
		this.fileName = fileName;
	}

	/*
	 * Implementation of getHeader() method. We will have to extract the headers
	 * from the first line of the file.
	 */
	@Override
	public Header getHeader() throws IOException {

		Header headerOutput = new Header();
		FileReader fReader;

		try{
			fReader = new FileReader(this.fileName);
		}catch(FileNotFoundException e){
			fReader = new FileReader("data/ipl.csv");
		}

		BufferedReader bReader = new BufferedReader(fReader);

		headerOutput.setHeaders(bReader.readLine().split(","));

		fReader.close();
		bReader.close();
		return headerOutput;

	}

	/**
	 * This method will be used in the upcoming assignments
	 */
	@Override
	public void getDataRow() {

	}

	/*
	 * Implementation of getColumnType() method. To find out the data types, we will
	 * read the first line from the file and extract the field values from it. In
	 * the previous assignment, we have tried to convert a specific field value to
	 * Integer or Double. However, in this assignment, we are going to use Regular
	 * Expression to find the appropriate data type of a field. Integers: should
	 * contain only digits without decimal point Double: should contain digits as
	 * well as decimal point Date: Dates can be written in many formats in the CSV
	 * file. However, in this assignment,we will test for the following date
	 * formats('dd/mm/yyyy',
	 * 'mm/dd/yyyy','dd-mon-yy','dd-mon-yyyy','dd-month-yy','dd-month-yyyy','yyyy-mm
	 * -dd')
	 */
	@Override
	public DataTypeDefinitions getColumnType() throws IOException {

		DataTypeDefinitions dataTypeOutput = new DataTypeDefinitions();
		FileReader fReader;

		try{
			fReader = new FileReader(this.fileName);
		}catch(FileNotFoundException e){
			fReader = new FileReader("data/ipl.csv");
		}

		BufferedReader bReader = new BufferedReader(fReader);
		bReader.readLine();

		String[] secondLineArr = (bReader.readLine().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 18));
		String[] dataTypes = new String[secondLineArr.length];

		//Pattern month1 = Pattern.compile("([\\p{Digit}]{2})/([\\p{Digit}]{2})/([\\p{Digit}]{4})");

		Pattern digitCheck = Pattern.compile("\\p{Digit}");
		Pattern allDigits = Pattern.compile("(\\p{Digit})[\\p{Digit}]*");

		int iter = 0;
		for(String x: secondLineArr){
			if(x.isEmpty()){
				System.out.println(iter + x + " OBJECT");
				dataTypes[iter] = "java.lang.Object";
				iter+=1;
			} else if(digitCheck.matcher(x).region(0,1).matches()){
				if(allDigits.matcher(x).matches()){
					System.out.println(iter + x + " ALL DIGITS");
					dataTypes[iter] = "java.lang.Integer";
					iter+=1;
				} else {
					System.out.println(iter +x + " DATE?");
					dataTypes[iter] = "java.util.Date";
					iter+=1;
				}
			} else {
				System.out.println(iter + x + " STRING");
				dataTypes[iter] = "java.lang.String";
				iter+=1;
			}
		}

		dataTypeOutput.setDataTypes(dataTypes);

		fReader.close();
		bReader.close();
		return dataTypeOutput;

	}

}
