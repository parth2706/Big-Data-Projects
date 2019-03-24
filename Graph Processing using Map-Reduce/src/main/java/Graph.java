import java.io.*;
import java.util.Vector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configured;


class Vertex implements Writable {
    public short tag;                 // 0 for a graph vertex, 1 for a group number
    public long group;                // the group where this vertex belongs to
    public long VID;                  // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors
    /* ... */
    
    public Vertex()
    {
    	this.tag=0;
    	this.VID=0;
    	this.group=0;
    	this.adjacent=new Vector<Long>();
    }
    
    
    Vertex (short tag, long group, long VID, Vector<Long> adjacent)
    {
    	
    	this.tag = tag;
    	this.group = group;
    	this.VID = VID;
    	this.adjacent = adjacent;
    }
    Vertex(short tag, long group)
    {
    	this.tag=tag;
    	this.group=group;
    	this.VID=0;
    	this.adjacent = new Vector<Long>();
    }
    
    short getTag()
    {
    	return tag;
    }
    
    long getGroup()
    {
    	return group;
    }

    long getVID()
    {
    	return VID;
    }
   
    Vector<Long> getAdjacent()
    {
    	return adjacent;
    }
    
    // remaining Vector<Long> to write on dataoutput object 
    public void write ( DataOutput out ) throws IOException {
		out.writeShort(tag);
        out.writeLong(group);
        out.writeLong(VID);
        
        
        LongWritable size = new LongWritable(adjacent.size());
        size.write(out);
        
        for(Long adjacent1: adjacent)
        {
        	out.writeLong(adjacent1);;
        }
        
    }
 
    // remaining Vector<Long> to read form datainput object
    public void readFields (DataInput in) throws IOException {
    	 
    	tag = in.readShort();
    	group = in.readLong();
    	VID = in.readLong();
    	adjacent.clear();  
    	LongWritable length = new LongWritable();
    	length.readFields(in);
    	
    	for(int i=0;i<length.get();i++)
    	{
    		LongWritable adjacent1 = new LongWritable();
    		adjacent1.readFields(in);
    		adjacent.add(adjacent1.get());
    	}
    }
}



public class Graph{  

	
	// First Map-Reduce Job
	 static class FirstMapper extends Mapper<Object,Text,LongWritable,Vertex > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
        	 String values = value.toString();
             String[] v = values.split(",");
             // getting the 1st value from each line which is the vertex id
             //long VID = s.nextLong();
             long VID = Long.parseLong(v[0]);
             Vector<Long> adjacent = new Vector<Long>();
             for (int i = 1; i < v.length; i++ )
             {
                 adjacent.add(Long.parseLong(v[i]));
             }
             context.write(new LongWritable(VID), new Vertex((short) 0,VID,VID,adjacent));
        }
    }
	
	// Second Map-Reduce Job
	 static class GroupMapper extends Mapper<LongWritable,Vertex,LongWritable,Vertex > {
        @Override
        public void map (LongWritable key, Vertex value, Context context )
                        throws IOException, InterruptedException {
            
        	context.write(new LongWritable(value.getVID()),value);
        	for(Long l: value.getAdjacent())
        	{
        		context.write(new LongWritable(l),new Vertex((short)1, value.getGroup()));
        	}
        }
    }
	
	 static class GroupReducer extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {
        @Override
        public void reduce ( LongWritable key, Iterable<Vertex> values, Context context )
                           throws IOException, InterruptedException {
            long m = Long.MAX_VALUE;
            Vector<Long> adj = new Vector<Long>();
            for (Vertex v: values) {
            	  if(v.getTag() == 0)
            	  {
            		  adj = new Vector<Long>(v.getAdjacent());
            	  }
            	//  m = (m <= v.getGroup()) ? m : v.getGroup();
            	  m = Math.min(m, v.getGroup());
            }
            context.write(new LongWritable(m), new Vertex((short)0,m,key.get(),adj));
//                sum += v.get();
//                System.out.print(key);  System.out.print("  ");
//                System.out.println(v.get()+" "+sum);    
        }
    }
	
	// Final Map-Reduce Job
	 static class AggregationMapper extends Mapper<LongWritable,Vertex,LongWritable,IntWritable > {
        @Override
        public void map ( LongWritable key, Vertex value, Context context ) throws IOException, InterruptedException {
            
            context.write(key,new IntWritable(1));
        }
    }
	
	 static class AggregationReducer extends Reducer<LongWritable,IntWritable,LongWritable,IntWritable> {
        @Override
        public void reduce ( LongWritable key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v: values) {
                sum += v.get();
//                System.out.print(key);  System.out.print("  ");
//                System.out.println(v.get()+" "+sum);
            }
            context.write(key,new IntWritable(sum));
        }
    }
	
	 
	
	
    /* ... */

    public static void main ( String[] args ) throws Exception {
        
    
    	Job job = Job.getInstance();
        job.setJobName("MyJob");
        job.setJarByClass(Graph.class);
        job.setMapperClass(FirstMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
       
        
       job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"/f0"));
        job.waitForCompletion(true);
        /* ... First Map-Reduce job to read the graph */
       
        for (short i = 0; i < 5; i++ ) {
        	Job job2=Job.getInstance();
        	job2 = Job.getInstance();
            job2.setJobName("MyJob1");
            job2.setJarByClass(Graph.class);
            job2.setMapperClass(GroupMapper.class);
            job2.setReducerClass(GroupReducer.class);
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Vertex.class);
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Vertex.class);
            
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(job2,new Path(args[1]+"/f"+i));
            FileOutputFormat.setOutputPath(job2,new Path(args[1]+"/f"+(i+1)));
            job2.waitForCompletion(true);
        }
        
	
        Job	job3 = Job.getInstance();
       // job = new Job(new Configuration(), "MyJob2");
        job3.setJobName("MyJob2");
        job3.setJarByClass(Graph.class);
        job3.setMapperClass(AggregationMapper.class);
        job3.setReducerClass(AggregationReducer.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(IntWritable.class);
        
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job3,new Path(args[1]+"/f5"));
        FileOutputFormat.setOutputPath(job3,new Path(args[2]));
        /* ... Final Map-Reduce job to calculate the connected component sizes */
        job3.waitForCompletion(true);
        }
    //}
}
