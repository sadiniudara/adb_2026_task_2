import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 
 * @author Martin Schaeler martin.schaeler@plus.ac.at
 * 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
*/

public class NoLiveLocks {
	public static int num_taks=30;
	public static int num_queries_per_task = 3;
	public static int num_threads = 4;
	
	Connection con;
	Random rand = new Random(123456);
	int num_tuple = 50000;
	double max_value = 50.0f;
	AtomicInteger tasks_finished;
	AtomicInteger total_restarts = new AtomicInteger(0);
	AtomicInteger max_restarts = new AtomicInteger(0);
	AtomicInteger exclusive_runs = new AtomicInteger(0);

	/** After this many aborts a task stops competing and runs alone (aging). */
	public static int max_shared_attempts = 3;
	/**
	 * Admission gate. Normal attempts hold the read lock and run concurrently.
	 * A starving task takes the write lock, so it runs alone and cannot be a deadlock victim.
	 * Fair mode: once a starving task waits, new attempts queue behind it, so it cannot starve here either.
	 */
	final ReentrantReadWriteLock gate = new ReentrantReadWriteLock(true);
	
	public NoLiveLocks(){
		tasks_finished = new AtomicInteger(0);
	}
	
	void executeSQL(String sql, Connection con){
		java.sql.Statement stmt;
		try {
			stmt = con.createStatement();
			stmt.execute(sql);
			System.out.println("Executing "+sql);
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	void cerateDB(){	
		try {
			Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
			executeSQL("DROP TABLE IF EXISTS blocking_data;",con);
			executeSQL("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);",con);
			
			PreparedStatement ps;
			con.setAutoCommit(false);
			ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)",ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			for(int i=0;i<num_tuple;i++){
				ps.setInt(1, i);
				ps.setDouble(2,rand.nextDouble()*max_value);
				ps.setInt(3, 0);
				ps.addBatch();
				ps.clearParameters();
			}
			
			int[] results = ps.executeBatch();
			con.commit();
			con.close();
			System.out.println(Arrays.toString(results));
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/** Optional args: [num_tasks] [num_queries_per_task] [num_threads] [max_shared_attempts] */
	public static void main(String[] args){
		if(args.length>0) num_taks = Integer.parseInt(args[0]);
		if(args.length>1) num_queries_per_task = Integer.parseInt(args[1]);
		if(args.length>2) num_threads = Integer.parseInt(args[2]);
		if(args.length>3) max_shared_attempts = Integer.parseInt(args[3]);

		//Create DB
		NoLiveLocks b = new NoLiveLocks();
		b.cerateDB();
		
		//Create task and their queries
		ArrayList<NoLiveLocks.Blocker> my_tasks = new ArrayList<NoLiveLocks.Blocker>(num_taks);
		for(int task=0;task<num_taks;task++){
			my_tasks.add(b.new Blocker(task));
		}
		
		//Execute tasks
		ThreadPoolExecutor executor = 
				  (ThreadPoolExecutor) Executors.newFixedThreadPool(num_threads);//default 4
		double start, stop;
		start = System.currentTimeMillis();
		for(Blocker task : my_tasks) {
			executor.execute(task);
		}
		executor.shutdown();
		
		//wait for the tasks to finish
		while(!executor.isTerminated()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		stop = System.currentTimeMillis();
		System.out.println("[DONE] Task execution after roughly "+(stop-start)+" ms finished: "+b.tasks_finished+" of "+num_taks
				+" | restarts (aborts): "+b.total_restarts+" | max restarts of one task: "+b.max_restarts
				+" | exclusive runs: "+b.exclusive_runs);
	}

	/** Deadlock (40P01) and serialization failure (40001) are the aborts worth retrying. */
	static boolean isRetryable(SQLException e){
		String state = e.getSQLState();
		return "40P01".equals(state) || "40001".equals(state);
	}
	
	/**
	 * This is simply a task attempting to hold many write locks for a long time and updating major parts of the database.
	 *
	 */
	public class Blocker implements Runnable  {
		final int task;
		final double [] start_range;
		final double [] stop_range;
		
		public Blocker(int id){
			this.task = id;
			this.start_range = new double[num_queries_per_task];
			this.stop_range  = new double[num_queries_per_task];
			for(int q=0;q<num_queries_per_task;q++) {
				stop_range[q]  = rand.nextDouble()*max_value;
				start_range[q] = rand.nextDouble()*stop_range[q];
			}
		}
		
		/**
		 * Runs the transaction until it commits, while avoiding live locks:
		 *  1. Ordered pre-locking: first lock every row the queries can touch, in data_id order.
		 *     All tasks lock in the same order, so they cannot wait on each other in a cycle.
		 *  2. Random backoff after an abort, so the same tasks do not collide again right away.
		 *  3. Aging: after max_shared_attempts aborts, the task runs alone (write lock on the gate)
		 *     and is guaranteed to commit.
		 */
		@Override
		public void run() {
			Connection con = null;//XXX - Yes there needs to be a separate Connection per blocking taks. Otherwise strange things can happen
			int attempt = 0;
			try {
				con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
				con.setAutoCommit(false);
				con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
				PreparedStatement ps = con.prepareStatement("UPDATE blocking_data SET data_value = ?,  modified_by = "+task+" WHERE data_value >= ? AND data_value <= ?;",ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
				PreparedStatement lock = con.prepareStatement(buildLockSql());

				while(true) {
					boolean exclusive = attempt >= max_shared_attempts;
					if(exclusive) gate.writeLock().lock(); else gate.readLock().lock();
					try {
						// 1. lock all rows of all ranges in a global order (data_id)
						for(int q=0; q<num_queries_per_task; q++) {
							lock.setDouble(2*q+1, start_range[q]);
							lock.setDouble(2*q+2, stop_range[q]);
						}
						lock.executeQuery().close();
						// 2. the actual updates (rows are already ours)
						for(int q=0; q<num_queries_per_task; q++) {
							ps.setDouble(1, (float) (rand.nextDouble()*max_value));//some value, which one is not important.
							ps.setDouble(2, start_range[q]);
							ps.setDouble(3, stop_range[q]);
							ps.execute();
						}
						con.commit();
						if(exclusive) exclusive_runs.incrementAndGet();
						break; // committed
					} catch (SQLException e) {
						try { con.rollback(); } catch (SQLException ignore) {}
						if(!isRetryable(e)) throw e;
						attempt++;
						total_restarts.incrementAndGet();
						System.err.println("Task "+task+" aborted ("+e.getSQLState()+"), restart #"+attempt
								+(attempt >= max_shared_attempts ? " -> will run exclusively" : ""));
					} finally {
						if(exclusive) gate.writeLock().unlock(); else gate.readLock().unlock();
					}
					backoff(attempt);
				}
				int num_finished = tasks_finished.incrementAndGet();
				updateMax(max_restarts, attempt);
				System.out.println("Done task "+task+" after "+attempt+" restart(s). I am finisher number "+num_finished);
			} catch (SQLException e) {
				System.err.println("Task "+task+" failed permanently: "+e.getMessage());
			} finally {
				if(con != null) try { con.close(); } catch (SQLException ignore) {}
			}
		}

		/** SELECT ... WHERE (range_1 OR ... OR range_n) ORDER BY data_id FOR UPDATE */
		String buildLockSql(){
			StringBuilder sb = new StringBuilder("SELECT data_id FROM blocking_data WHERE ");
			for(int q=0; q<num_queries_per_task; q++) {
				if(q>0) sb.append(" OR ");
				sb.append("(data_value >= ? AND data_value <= ?)");
			}
			return sb.append(" ORDER BY data_id FOR UPDATE").toString();
		}
	}

	/** Random exponential backoff (capped) so retries of conflicting tasks spread out. */
	static void backoff(int attempt){
		if(attempt == 0) return;
		long max = Math.min(1000L, 50L << Math.min(attempt, 5));
		try { Thread.sleep(ThreadLocalRandom.current().nextLong(max)); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
	}

	static void updateMax(AtomicInteger max, int value){
		int cur;
		while(value > (cur = max.get()) && !max.compareAndSet(cur, value)) {}
	}

}
