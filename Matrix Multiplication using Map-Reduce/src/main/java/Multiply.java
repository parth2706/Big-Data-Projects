import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;


class Elem implements Writable{
	short tag;
	int index;
	//int indexj;
	double value;
	
	Elem(){
		
	}
	
	Elem(short n, int i, double d){
		tag=n;
		index=i;
		value=d;
	}

	public void write ( DataOutput out ) throws IOException {
        out.writeShort(tag);
        out.writeInt(index);
        out.writeDouble(value);
    }

    public void readFields ( DataInput in ) throws IOException {
    	tag = in.readShort();
    	index = in.readInt();
    	value = in.readDouble();
    }
	
//    public String toString() {
//    	return new String(value+"");
//    }
}

class Pair implements WritableComparable<Pair>{
	int i;
	int j;

	Pair(){
		
	}
	
	Pair(int indexm, int indexn)
	{
		i=indexm;
		j=indexn;
	}
	
	public int compareTo (Pair p)
	{
		if (i > p.i) {
			return 1;
		} else if ( i < p.i) {
			return -1;
		} else {
			if(j > p.j) {
				return 1;
			} else if (j < p.j) {
				return -1;
			}
		}
        return 0;
	}
	
	public void write ( DataOutput out ) throws IOException {
		out.writeInt(i);
        out.writeInt(j);     
    }

    public void readFields ( DataInput in ) throws IOException {
    	i = in.readInt();
    	j = in.readInt(); 	
    }
    
    public String toString() {
    	return i+" "+j+" ";
    }
}

public class Multiply {

   
    	
	public static class MapperForMatrix1 extends Mapper<Object,Text,IntWritable,Elem > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int i=s.nextInt();
            int j=s.nextInt();
            double v=s.nextDouble();
            context.write(new IntWritable(j),new Elem((short)0,i,v));
            s.close();
        }
    }

    public static class MapperForMatrix2 extends Mapper<Object,Text,IntWritable,Elem > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
        	Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int i=s.nextInt();
            int j=s.nextInt();
            double v=s.nextDouble();
            context.write(new IntWritable(i),new Elem((short)1,j,v));
            s.close();
        }
    }

    public static class ReducerForResult extends Reducer<IntWritable,Elem,Pair,DoubleWritable> {
        static Vector<Elem> matrixM = new Vector<Elem>();  // A
        static Vector<Elem> matrixN = new Vector<Elem>();  // B
        @Override
        public void reduce ( IntWritable key, Iterable<Elem> values, Context context )
                           throws IOException, InterruptedException {
//        	matrixM.clear();
//        	matrixN.clear();
//            for (Elem v: values)
//                if (v.tag == 0)
//                	matrixM.add(v);
//                else matrixN.add(v);
        	matrixM.clear();
        	matrixN.clear();
        	
            Configuration conf = context.getConfiguration();
    		
    		for(Elem element : values) {
    			
    			
    			Elem tem = ReflectionUtils.newInstance(Elem.class, conf);
    			ReflectionUtils.copy(conf, element, tem);
    			
    			if (element.tag == 0) {
    				matrixM.add(tem);
    			} else  {
    				matrixN.add(tem);
    			}
    }
            
//            for(Elem m: matrixM)
//            {
//            	System.out.println("Index:"+m.index+" Value:"+m.value+" Tag:"+m.tag);
//            }
//            
//            for(Elem n: matrixN)
//            {
//            	System.out.println("Index:"+n.index+" Value:"+n.value+" Tag:"+n.tag);
//            }
            
            for ( Elem m: matrixM )
                {
            	System.out.println("ReducerForResult Starts here");
            	for ( Elem n: matrixN ) {
                    System.out.println("m.index:"+m.index+" ,n.index:"+n.index+" ,m.value:"+m.value+" , n.value:"+n.value);
            		context.write(new Pair(m.index,n.index),new DoubleWritable(m.value*n.value));
            	}
                   System.out.println("ReducerForResult Ends here");
                }
        }
    }

    public static class MyMapper3 extends Mapper<Pair,DoubleWritable,Pair,DoubleWritable> {
        @Override
        public void map ( Pair key, DoubleWritable value, Context context )
                        throws IOException, InterruptedException {
        	System.out.println("MyMapper3 Starts here");
        	System.out.print(key);
            System.out.println(value.get());
            System.out.println("MyMapper3 Ends here");
            context.write(key,value);
        }
    }

    public static class MyReducer3 extends Reducer<Pair,DoubleWritable,Pair,DoubleWritable> {
        @Override
        public void reduce ( Pair key, Iterable<DoubleWritable> values, Context context )
                           throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable v: values) {
                sum += v.get();
                System.out.println("Reducer 3 starts");
                System.out.print(key);  System.out.print("  ");
                System.out.println(v.get()+" "+sum);
                System.out.println("Reducer 3 ends");
            }
            context.write(key,new DoubleWritable(sum));
        }
    }
    
    
        public static void main ( String[] args ) throws Exception {
        	Job job = Job.getInstance();
            job.setJobName("JoinJob");
            job.setJarByClass(Multiply.class);
            job.setOutputKeyClass(Pair.class);
            job.setOutputValueClass(DoubleWritable.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Elem.class);
            job.setReducerClass(ReducerForResult.class);
            job.setOutputFormatClass( SequenceFileOutputFormat.class); 
            MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,MapperForMatrix1.class);
            MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,MapperForMatrix2.class);
            FileOutputFormat.setOutputPath(job,new Path(args[2]));
            job.waitForCompletion(true);
            
            int code = job.waitForCompletion(true) ? 0 : 1;
            if (code == 0) {
            
            Job job1 = new Job(new Configuration(), "MyJob1");
            
           
            job1.setJarByClass(Multiply.class);
            
            job1.setOutputKeyClass(Pair.class); // Pair.class
            
            job1.setOutputValueClass(DoubleWritable.class);
           
            job1.setMapOutputKeyClass(Pair.class);  // Pair.class
            
            job1.setMapOutputValueClass(DoubleWritable.class);
            
            job1.setMapperClass(MyMapper3.class);
            
            job1.setReducerClass(MyReducer3.class);
           
            job1.setInputFormatClass( SequenceFileInputFormat.class);
            
            job1.setOutputFormatClass(TextOutputFormat.class);
            
            
           
            FileInputFormat.setInputPaths(job1,new Path(args[2]));
            
            FileOutputFormat.setOutputPath(job1,new Path(args[3]));
            job1.waitForCompletion(true);
        }

    }
}