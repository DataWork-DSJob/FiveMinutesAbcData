Flink类加载器,都是FlinkUserCodeClassLeader抽象类的子类; 包括2个
- ChildFirstClassLoader, 这是默认策略, 默认情况,Flink会优先从用户jar中加载class;
- ParentFirstClassLoader


org.apache.flink.configuration.CoreOptions 定义里如下配置: 

classloader.check-leaked-classloader 		true
	Fails attempts at loading classes if the user classloader of a job is used after it has terminated.
	This is usually caused by the classloader being leaked by lingering threads or misbehaving libraries, which may also result in the classloader being used by other jobs.
	This check should only be disabled if such a leak prevents further jobs from running

classloader.fail-on-metaspace-oom-error		true
	Fail Flink JVM processes if 'OutOfMemoryError: Metaspace' is thrown while trying to load a user code class
	
classloader.parent-first-patterns.additional	none
	A (semicolon-separated) list of patterns that specifies which classes should always be resolved through the parent ClassLoader first. A pattern is a simple prefix that is checked against the fully qualified class name. These patterns are appended to "classloader.parent-first-patterns.default".
	
classloader.parent-first-patterns.default		java.;<wbr>scala.;<wbr>org.apache.flink.

classloader.resolve-order		child-first
	Defines the class resolution strategy when loading classes from user code, meaning whether to first check the user code jar ("child-first") or the application classpath ("parent-first"). The default settings indicate to load classes first from the user code jar, which means that user code jars can include and load different dependencies than Flink uses (transitively)



类加载的问题: 
1. 双亲委派问题和ChildFirst解决方案: 
- 双亲委派模型的好处是随着加载器的层次关系保证了被加载类的层次关系，从而保证了 Java 运行环境的安全性。但是在 Flink App 这种依赖纷繁复杂的环境中，双亲委派模型可能并不适用。例如，程序中引入的 Flink-Kafka Connector 总是依赖于固定的 Kafka 版本，用户代码中为了兼容实际使用的 Kafka 版本，会引入一个更低或更高的依赖。而同一个组件不同版本的类定义可能会不同(即使类的全限定名是相同的)，如果仍然用双亲委派模型，就会因为 Flink 框架指定版本的类先加载，而出现莫名其妙的兼容性问题，如 NoSuchMethodError、IllegalAccessError等。
- 鉴于此，Flink 实现了 ChildFirstClassLoader 类加载器并作为默认策略，它打破了双亲委派模型，使得用户代码的类先加载
* 场景总结:  
  希望用户代码总指定的class(jar)先加载使用, 而 双亲委派 无法实现这一点; 
  ChildFirst类加载 就是使得用户代码的类先加载, 这是flink的默认类加载策略; 




// flink_1.12_src 

abstract FlinkUserCodeClassLeader extends URLClassLoader {
	@Override
    public final Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        try {
            return loadClassWithoutExceptionHandling(name, resolve); {// FlinkUserCodeClassLoader.loadClassWithoutExceptionHandling()
				return super.loadClass(name, resolve);
			}
        } catch (Throwable classLoadingException) {
            classLoadingExceptionHandler.accept(classLoadingException);
            throw classLoadingException;
        }
    }
}


A variant of the URLClassLoader that first loads from the URLs and only after that from the parent. 
getResourceAsStream(String) uses getResource(String) internally so we don't override that

class ChildFirstClassLoader extends FlinkUserCodeClassLoader {

	@Override
    protected Class<?> loadClassWithoutExceptionHandling(String name, boolean resolve)throws ClassNotFoundException {
        // First, check if the class has already been loaded
		// 首先调用 findLoadedClass 方法检查全限定名 name 对应的类是否已加载过
        Class<?> c = findLoadedClass(name);
        if (c == null) {
            // check whether the class should go parent-first
			// 检查要加载的类是否以 alwaysParentFirstPatterns 集合中的前缀开头。如果是，则调用父类的 loadClassWithoutExceptionHandling 方法，以 Parent-First 的方式加载它
            for (String alwaysParentFirstPattern : alwaysParentFirstPatterns) {
                if (name.startsWith(alwaysParentFirstPattern)) {
                    return super.loadClassWithoutExceptionHandling(name, resolve);
                }
            }
            try {
                // check the URLs
				// 若加载的类不以 alwaysParentFirstPatterns 集合中的前缀开头，则调用父类 URLClassLoader 的 findClass 方法进行类加载
                c = findClass(name);
            } catch (ClassNotFoundException e) {
                // let URLClassLoader do it, which will eventually call the parent
                // 若调用 findClass() 方法失败, 最终再调用父类的 loadClassWithoutExceptionHandling 方法，以 Parent-First 的方式加载它
				c = super.loadClassWithoutExceptionHandling(name, resolve);
            }
        } else if (resolve) {
            resolveClass(c);
        }
        return c;
    }

}

关于 ChildFirst 的一些特性: 
* 用户如果仍然希望某些类"遵循祖制"，采用双亲委派模型来加载，则需要借助 alwaysParentFirstPatterns 集合来实现  classloader.parent-first-patterns.default
* classloader.parent-first-patterns.additional , 用户如果仍然希望某些类"遵循祖制"，采用双亲委派模型来加载，则可以通过该参数额外指定，并以分号分隔
* Flink 会将上述2种配置合并在一起，作为 alwaysParentFirstPatterns 集合

* Parent-First 类加载策略照搬双亲委派模型，也就是说，用户代码的类加载器是User ClassLoader，Flink 框架本身的类加载器是 Application ClassLoader，用户代码中的类先由 Flink 框架的类加载器加载，再由用户代码的类加载器加载。




// Regular URLClassLoader that first loads from the parent and only after that from the URLs.
FlinkUserCodeClassLoaders.ParentFirstClassLoader  extends FlinkUserCodeClassLoader {
	static {
            ClassLoader.registerAsParallelCapable();
        }
}




// FlinkUserCodeClassLoaders.create(
                    classLoaderResolveOrder,
                    libraryURLs,
                    FlinkUserCodeClassLoaders.class.getClassLoader(),
                    alwaysParentFirstPatterns,
                    classLoadingExceptionHandler,
                    checkClassLoaderLeak);
					
					


1. JM, TM的启动生成 ClassLoader的流程源码; 
// 调度器接到启动JobManagerRunner的命令  
// 在JobManager启动时, 
Dispatcher.createJobManagerRunner() {
	return CompletableFuture.supplyAsync(() -> {jobManagerRunnerFactory.createJobManagerRunner();}); {// DefaultJobManagerRunnerFactory.
		JobMasterServiceFactory jobMasterFactory = new DefaultJobMasterServiceFactory();
		return new JobManagerRunnerImpl();{
			userCodeLoader = classLoaderLease.getOrResolveClassLoader(jobGraph.getUserJarBlobKeys(), jobGraph.getClasspaths()).asClassLoader();{//BlobLibraryCacheManager$DefaultClassLoaderLease.getOrResolveClassLoader()
				return libraryCacheEntry.getOrResolveClassLoader(requiredJarFiles, requiredClasspaths); {
					URLClassLoader ucl=  createUserCodeClassLoader(jobId, libraries, classPaths);{//BlobLibraryCacheManager$LibraryCacheEntry.createUserCodeClassLoader
						return FlinkUserCodeClassLoaders.create();{
							// 核心判断, 在这里区分是用 ChildFirstClassLoader , 还是 ChildFirstClassLoader; 
							switch (resolveOrder) {
								case CHILD_FIRST:
									return childFirst(urls,parent,alwaysParentFirstPatterns,);{
										FlinkUserCodeClassLoader classLoader = new ChildFirstClassLoader(urls, parent, alwaysParentFirstPatterns, classLoadingExceptionHandler);
										return wrapWithSafetyNet(classLoader, checkClassLoaderLeak);
									}
								case PARENT_FIRST:
									return parentFirst(urls, parent, classLoadingExceptionHandler, checkClassLoaderLeak); {
										FlinkUserCodeClassLoader classLoader =  new ParentFirstClassLoader(urls, parent, classLoadingExceptionHandler);
										return wrapWithSafetyNet(classLoader, checkClassLoaderLeak);
									}
								default:
									throw new IllegalArgumentException("Unknown class resolution order: " + resolveOrder);
							}
						}
					}
					resolvedClassLoader = new ResolvedClassLoader(ucl);
				}
			}
			
		}
	}
}

// 2. 在Task 启动时; 

Task.run(){
	doRun(){//Task.doRun()
		userCodeClassLoader = createUserCodeClassloader();// Task.createUserCodeClassloader
			UserCodeClassLoader userCodeClassLoader = classLoaderHandle.getOrResolveClassLoader(requiredJarFiles, requiredClasspaths);
				return libraryCacheEntry.getOrResolveClassLoader(requiredJarFiles, requiredClasspaths);//BlobLibraryCacheManager$LibraryCacheEntry 
					URLClassLoader ucl=  createUserCodeClassLoader(jobId, libraries, classPaths);{//BlobLibraryCacheManager$LibraryCacheEntry.createUserCodeClassLoader
						return FlinkUserCodeClassLoaders.create();{
							// 核心判断, 在这里区分是用 ChildFirstClassLoader , 还是 ChildFirstClassLoader; 
							switch (resolveOrder) {
								case CHILD_FIRST:
									return childFirst(urls,parent,alwaysParentFirstPatterns,);{
										FlinkUserCodeClassLoader classLoader = new ChildFirstClassLoader(urls, parent, alwaysParentFirstPatterns, classLoadingExceptionHandler);
										return wrapWithSafetyNet(classLoader, checkClassLoaderLeak);
									}
								case PARENT_FIRST:
									return parentFirst(urls, parent, classLoadingExceptionHandler, checkClassLoaderLeak); {
										FlinkUserCodeClassLoader classLoader =  new ParentFirstClassLoader(urls, parent, classLoadingExceptionHandler);
										return wrapWithSafetyNet(classLoader, checkClassLoaderLeak);
									}
								default:
									throw new IllegalArgumentException("Unknown class resolution order: " + resolveOrder);
							}
						}
					}
	}
}














