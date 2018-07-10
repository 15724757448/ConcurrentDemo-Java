package com.oldjim.concurrent.executor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;


/**
 * 线程池 
 * 此版本我们进行一波优化，主要打算解决的问题有：
 * 			问题1：延迟初始化线程，当有请求进入时再初始化线程
 * 			问题2：线程池作为线程容器，要对线程的创建做的更灵活一些
 * 			问题3：增加拒绝策略，在线程池不可用时拒绝任务
 * 			问题4：暂不解决
 * 			问题5：完善异常处理逻辑
 * 			问题6：增加回收策略
 * 			问题7：暂不解决
 * 			问题8：增加核心线程/非核心线程概念
 * 			问题9：暂不解决
 * 			问题10：通过线程中断解决线程调度问题
 * 			问题11：暂不解决
 * 1.线程默认直接初始化 						浪费资源
 * 2.线程是new出来的							耦合性高
 * 3.任务队列满了之后没有拒绝策略				功能不完善
 * 4.线程池大小是初始化时固定的不可动态修改		功能不完善
 * 5.任务异常时没有处理逻辑						功能不完善
 * 6.线程空闲时没有回收策略						浪费资源 
 * 7.无统计功能								功能不完善
 * 8.线程池不可动态扩容（核心线程/非核心线程）		功能不完善
 * 9.扩展性差，基本上没有扩展点					功能不完善
 * 10.无法中断任务							功能不完善
 * 11.可监视及动态调整任务队列（如持久化队列）		新需求（ThreadPoolExecutor中不具备的功能）
 * @author oldJim
 * @version 0.2 
 * 
 * 下面我们通过分析面临的问题来思考各个问题的解决方案
 * 问题1的解决思路初步设想是将初始化线程的入口操作放到execute()中去，有任务进来在初始化线程
 * 		所以现在我们需要一个变量来记录工作线程的数量，实现方法有两个
 * 		第一个是使用workers.size()来获取工作线程数量，
 * 			方案一需要依赖Set的size()实现，在非线程安全的Set类中，我们需要加锁来保证获取正确的数据。而在线程安全的Set类中，size()方法的实现往往非常复杂。
 * 		第二个是增加一个volatile数字或原子数字workerCount来记录工作线程数量，
 * 			方案二利用CAS保证线程数量的正确性，在需要临界区的时候加锁保证竞态条件下数据的正确性？
 * 			思考：是否存在竞态条件？
 * 			1：workerCount与workers是否有独占的原子操作？1.1workerCount与workers两者任一的改动是否是需要锁定另一个资源？1.2两者同时更改是否是原子性的？
 * 				暂时的考虑是1.1：workerCount的更改不需要判断workers的状态，workers的更改也不需要workerCount的状态，比如说遍历workers的条件是workers.size>0而不是workerCount>0
 * 						  1.2：workerCount和workers.size()允许有一段时间是不一致的。
 * 			2:workerCount与runStatus是否有独占的原子操作？
 * 				我认为有，workerCount的增加的条件必须为runStatus是运行状态的。所以这部分的操作需要加锁执行，考虑是否可优化呢（long高低位保证原子性）
 * 		此版本暂时的解决思路是增加workerCount字段记录工作线程数量
 * 问题2的解决思路是将使用与创建解耦，使用ThreadFactory来创建线程
 * 问题3的解决思路是增加拒绝策略
 * 问题4涉及到在运行期间修改参数，暂时不实现此功能
 * 问题5是在用户任务中抛出异常时的处理逻辑，目前简单try-catch捕获并打印异常信息
 * 问题6首先需要定义什么情况算是线程空闲，此版本定义的逻辑为X时间内从任务队列中获取不到任务，则线程空闲
 * 		当线程有回收就意味着有线程的发放。
 * 		目前线程的回收/发放想到的有三种实现
 * 								回收											发放
 * 			方案1（JDK实现）	    线程终止	   								重新创建线程
 * 			方案2（优化方案1）	    线程阻塞              								    唤醒线程
 * 			方案3（优化方案2）	短时间内阻塞线程，超时后线程终止				先唤醒阻塞，无阻塞线程则重新创建线程
 * 		此版本使用方案1，简单实现
 * 		
 * 问题7我们有时需要线程池执行的总任务数，每个线程成功执行的任务数量，失败的任务数量等，此版本暂时不实现此功能
 * 问题8首先需要定义什么样的线程为非核心线程，此版本定义非核心线程如下，开启条件：任务队列满开启非核心线程。关闭条件:线程为空闲线程
 * 		初步方案为监听队列状态，队列满了则使用问题6中的方案一管理线程的收发
 * 问题9在任务的开始和结束等可扩展的地方可以设置扩展点，扩展点暂不实现
 * 问题10在上一个版本中，我们使用共享内存的方式来实现线程之间的调度，但是当线程阻塞时无法使用此方法来调度线程，所以此版本我们需要引入线程协作来实现阻塞状态下的线程调度
 * 问题11是在使用无界队列时由于没有设置队列大小，导致内存飙高的一种解决思路---持久化任务队列 此版本我们暂不实现
 * 
 * 
 * 
 */
public class OJThreadPoolExecutor2 extends AbstractExecutorService{
	
	/**
	 * 线程池状态,初始状态为正在运行
	 * 此版本的线程通信使用共享内存模式，通过runStatus来协调各线程状态
	 */
	private volatile int runStatus = RUNNING;
	/**
	 * 线程池线程个数
	 */
	private int poolSize ;
	/**
	 * 任务队列,保证线程安全
	 */
	private ConcurrentLinkedQueue<Runnable> workQueue;
	/**
	 * 读写锁，用于线程状态更改的相关锁定操作
	 */
	private ReentrantReadWriteLock rsLock = new ReentrantReadWriteLock();
	/**
	 * 读锁
	 */
	private ReadLock rsReadLock = rsLock.readLock();
	/**
	 * 写锁
	 */
	private WriteLock rsWriteLock = rsLock.writeLock();
	/**
	 * 主锁，用于workers的锁定操作
	 */
	private ReentrantLock mainLock = new ReentrantLock();
	/**
	 * 用于管理工作者，在shutdown时有并发风险，不能使用CopyOnWriteArraySet，因为CopyOnWriteArraySet遍历时使用的是快照，并不是最新数据
	 */
	private Set<Worker> workers = new HashSet<Worker>();
	/**
	 * 线程池的状态，用于实现线程的中断、终止
	 */
	private static final int RUNNING    = -1 ;
    private static final int SHUTDOWN   =  0 ;
    private static final int STOP       =  1 ;
    //TODO 现在没有 TIDYING 这个中间状态
//    private static final int TIDYING    =  2 ;
    private static final int TERMINATED =  3 ;
    
    public OJThreadPoolExecutor2(int poolSize){
    	this(poolSize, new ConcurrentLinkedQueue<Runnable>());
    }
    public OJThreadPoolExecutor2(int poolSize,ConcurrentLinkedQueue<Runnable> workQueue){
        if (poolSize <= 0 ){
        	throw new IllegalArgumentException();
        }
        if (workQueue == null ){
        	throw new NullPointerException();
        }
    	this.poolSize = poolSize;
    	this.workQueue = workQueue;
    	//初始化所有线程
    	initAllWorker();
    	//启动所有线程
    	startAllWorker();
    }
    /**
     * 启动所有线程
     */
    private void startAllWorker() {
		for (Worker w : workers) {
			w.thread.start();
		}
	}
	/**
     * 初始化工作者
     */
    private void initAllWorker() {
    	for (int i = 0; i < poolSize; i++) {
			addWorker();
		}
	}
    /**
     * 添加工作者到工作队列
     */
	private void addWorker() {
		Worker worker = new Worker();
		workers.add(worker);
	}
	/**
     * 工作者-有工作任务
     */
    private final class Worker implements Runnable{
    	Thread thread ;
    	
    	public Worker() {
    		this.thread = new Thread(this);
		}

		/**
    	 * 工作任务
    	 */
		@Override
		public void run() {
			runWorker();
		}
		//TODO 放在哪？
		public void runWorker() {
			//循环执行任务
			while (true) {
				int rs ;
				//是否可以继续执行
				boolean isCanExecute = false;
				Runnable task = null ;
				//获取线程池状态,需要加锁保证
				try {
					rsReadLock.lock();
					rs = runStatus;
					//可执行状态
					if(isRunning(rs) && (task = getTask()) != null){
						isCanExecute = true;
					}else{
						//线程池状态为不可执行时跳出循环
						isCanExecute = false;
					}
				}  finally {
					rsReadLock.unlock();
				}
				if(isCanExecute){
					//执行方法
					task.run();
				}else{
					if(!isRunning(rs)){
						mainLock.lock();
						try {
							workers.remove(this);
						} finally {
							mainLock.unlock();
						}
						tryTerminate();
						return;
					}
				}
			}
		}
    }
    
    private boolean isRunning(int rs) {
		return rs == RUNNING;
	}
    
	public void tryTerminate() {
		if(workers.isEmpty() && !isRunning(runStatus)){
			//不用加锁，有可能造成多次修改，不影响结果
			this.runStatus = TERMINATED; 
		}
	}
	/**
	 * 关闭线程池
	 */
	@Override
	public void shutdown() {
		rsWriteLock.lock();
		try {
			//修改线程池状态
			runStatus = SHUTDOWN;
		} finally {
			rsWriteLock.unlock();
		}
	}
	/**
	 * 获取任务
	 * @return
	 */
	private Runnable getTask() {
		return workQueue.poll();
	}
	/**
	 * 尝试停止所有的活动执行任务、暂停等待任务的处理，并返回等待执行的任务列表。在从此方法返回的任务队列中排空（移除）这些任务。
	 * 此版本使用共享内存调度线程，暂时无法实现提交的command中的任务中断
	 */
	@Override
	public List<Runnable> shutdownNow() {
		rsWriteLock.lock();
		try {
			//修改线程池状态
			runStatus = STOP;
		} finally {
			rsWriteLock.unlock();
		}
		List<Runnable> unExecuteList = new ArrayList<Runnable>(workQueue);
		workQueue.clear();
		return unExecuteList;
	}
	/**
	 * 线程池是否关闭
	 */
	@Override
	public boolean isShutdown() {
		return runStatus == SHUTDOWN;
	}
	/**
	 * 线程池是否终止
	 */
	@Override
	public boolean isTerminated() {
		return runStatus == TERMINATED;
	}
	/**
	 * 休眠查询线程池是否终止
	 */
	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit)
			throws InterruptedException {
		int rs = runStatus;
		boolean isLoop = true;
		while (isLoop) {
			if (rs == TERMINATED){
				return true;
			}
			//线程休眠
			unit.sleep(timeout);
			isLoop =false;
			
		}
		return false;
	}
	/**
	 * 执行任务
	 */
	@Override
	public void execute(Runnable command) {
		rsReadLock.lock();
		try {
			workQueue.offer(command);
		} finally{
			rsReadLock.unlock();
		}
	}

}
