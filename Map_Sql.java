// import libraries

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Map_Sql {

	public static void main (String[] args) throws Exception {

		// Check if DISCOUNT and QUANTITY are in the accepted ranges
		if ( (Float.parseFloat(args[2]) >= 0.10) || (Float.parseFloat(args[2]) <= 0.01) || (Integer.parseInt(args[3]) >= 51) || (Integer.parseInt(args[3]) <= 9) ) {
			System.out.println("INCORRECT PARAMETERS: Discount->"+args[2]+" | Quantity->"+args[3]+ "\n0.02<=DISCOUNT<=0.09 \n 10<=QUANTITY<=50");
			System.exit(-1);
		}

		Configuration conf = new Configuration();

		// give user parameters to the configurator
		// so that the Map can use them
		conf.set("parameter1", args[2]);
		conf.set("parameter2", args[3]);

		Job job = Job.getInstance(conf, "sql map job");
		job.setJarByClass(Map_Sql.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


	public static class Map extends Mapper<Object, Text, Text, MapWritable> {
		// map output -> (Key, Value)
		// Key -> partkey (text)
		// Value -> MapWritable -> ( (Key, Value), (Key, Value) )
		//							 (quantity, integer), (discount, float)

		private Text l_partkey = new Text();
		
		private MapWritable map_output = new MapWritable();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			/*
			str[1] is the partkey
			str[4] is quantity
			str[5] is the extendedprice
			str[6] is the discount
			*/

			// get parameters from command-line
			Configuration conf = context.getConfiguration();
			String param1 = conf.get("parameter1");
			String param2 = conf.get("parameter2");
			float DISCOUNT = Float.parseFloat(param1);
			int QUANTITY = Integer.parseInt(param2);

			//parse data from file
			String line = value.toString();
			String str[] = line.split("\\|");

			// normalize part key length for better sorting
			if (str[1].length()<5) {
				int temp = 5 - str[1].length();
				String str_temp;
				for (int i = 0; i < temp; i++) {
					str[1] = "0"+str[1];
				}
			}

			// discount and quantity from data
			Float l_disc = Float.parseFloat(str[6]);
			Integer l_quant = Integer.parseInt(str[4]);

			/* check if discount and quantity is in desired range */
			if ( (l_disc <= DISCOUNT+0.01) && (l_disc >= DISCOUNT-0.01) && (l_quant >= QUANTITY) ) {
				l_partkey.set(str[1]);
				
				//convert string to float
				float l_extendedprice = Float.parseFloat(str[5]);
				float total_discount = (l_extendedprice * l_disc);

				// assign values to the Map output (key, VALUE)
				map_output.put(new Text("quantity"), new Text(Integer.toString(l_quant)));
				map_output.put(new Text("tot_discount"), new Text(Float.toString(total_discount)));
			}

			context.write(l_partkey, map_output);
		}
	}

	public static class Reduce extends Reducer<Object, MapWritable, Text, Text> {
	
		// overriding reduce method(runs each time for every key )
		public void reduce(Object key, Iterable<MapWritable> values, Context context)
			throws IOException, InterruptedException  {

			MapWritable r_output = new MapWritable();

			// initialize variables
			int sum_quantity = 0;
			float avg_discount = 0;
			
			// Variable l is a counter used in average calculation later
			int l = 0;

			// iterate through the values of the Map output
			// Each value contains two <Key, Value> pairs.
			for (MapWritable val : values) {
				l++;
				for (MapWritable.Entry<Writable, Writable> entry : val.entrySet()) {
					if (entry.getKey().toString().equals("quantity")) {
						sum_quantity += Integer.parseInt(entry.getValue().toString());
					}
					if (entry.getKey().toString().equals("tot_discount")) {
						avg_discount += Float.parseFloat(entry.getValue().toString());
					}
				}
			}

			// calculate average
			float avg_discount_f = avg_discount / l;

			r_output.put(new Text("sum_quantity"), new Text(Integer.toString(sum_quantity)));
			r_output.put(new Text("avg_discount"), new Text(Float.toString(avg_discount_f)));

			// output to print
			// k_output: for the partkey
			// k_output: for the quantity and avg discount
			Text output = new Text("Total quantity of items: "+sum_quantity+" | Avg discount of item: "+avg_discount_f);
			Text k_output = new Text("Partkey_ID: "+key+ " | ");

			// this prevents printing an empty line at the beginning
			if (sum_quantity !=0 ) {

				// writes results
				context.write(k_output, output);
			}
		}
	}
}