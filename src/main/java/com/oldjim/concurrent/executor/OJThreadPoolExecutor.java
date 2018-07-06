package com.oldjim.concurrent.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * 线程池 
 * 
 * 1.线程默认直接初始化 						浪费资源
 * 2.线程是new出来的							耦合性高
 * 3.任务队列满了之后没有拒绝策略				功能不完善
 * 4.线程池大小是初始化时固定的不可动态修改		功能不完善
 * 5.任务异常时没有处理逻辑						功能不完善
 * 6.线程空闲时没有回收策略						浪费资源 
 * 7.无统计功能								功能不完善
 * 8.线程池不可动态扩容（核心线程/非核心线程）		功能不完善
 * 9.扩展性差，基本上没有扩展点					功能不完善
 * 10.可监视及动态调整任务队列（如持久化队列）		新需求（ThreadPoolExecutor中不具备的功能）
 * @author oldJim
 * @version 0.1 
 */
public class OJThreadPoolExecutor extends AbstractExecutorService{
	
	/**
	 * 线程池状态,初始状态为正在运行
	 * 此版本的线程通信使用共享内存模式，通过runStatus来协调各线程状态
	 */
	private volatile int runStatus = RUNNING;
	/**
	 * 线程池线程个数
	 */
	private int wc ;
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
	 * 用于管理工作者，在shutdown时有并发风险，使用CopyOnWriteArraySet
	 */
	private Set<Worker> workers = new CopyOnWriteArraySet<Worker>();
	/**
	 * 线程池的状态，用于实现线程的中断、终止
	 */
	private static final int RUNNING    = -1 ;
    private static final int SHUTDOWN   =  0 ;
    private static final int STOP       =  1 ;
    //TODO 现在没有 TIDYING 这个中间状态
//    private static final int TIDYING    =  2 ;
    private static final int TERMINATED =  3 ;
    
    public OJThreadPoolExecutor(int wc){
    	this(wc, new ConcurrentLinkedQueue<Runnable>());
    }
    public OJThreadPoolExecutor(int wc,ConcurrentLinkedQueue<Runnable> workQueue){
        if (wc <= 0 ){
        	throw new IllegalArgumentException();
        }
        if (workQueue == null ){
        	throw new NullPointerException();
        }
    	this.wc = wc;
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
    	for (int i = 0; i < wc; i++) {
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
						workers.remove(this);
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
