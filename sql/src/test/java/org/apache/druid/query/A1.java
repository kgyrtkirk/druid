package org.apache.druid.query;


import java.lang.StackWalker.Option;
import java.lang.StackWalker.StackFrame;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class A1
{

  interface U
  {
    default int g()
    {
      throw new RuntimeException();
    }
  }

  private static final StackWalker ExtendedWalker = getExtendedWalker();
  private static Method theLocalMethod;

  private static StackWalker getExtendedWalker()
  {
    Set<Option> opts = new HashSet<>();
    for (Option option :  Option.values()) {
      opts.add(option);
    }

    try {
      Class<?> liveStackFrameClass = Class.forName("java.lang.LiveStackFrame");
      Method method = liveStackFrameClass.getMethod("getStackWalker", Set.class);
      method.setAccessible(true);
      StackWalker extendedWalker = (StackWalker) method.invoke(null, opts);


      Class<?> liveStackFrameClass1 = Class.forName("java.lang.LiveStackFrameInfo");
      Method method1 = liveStackFrameClass1.getMethod("getLocals");
      method1.setAccessible(true);
      theLocalMethod = method1;


      return extendedWalker;
    }
    catch (Exception e) {
      e.printStackTrace();
      System.out.println("Falling back to standard walker");
      return StackWalker.getInstance(opts);

    }
  }

  public static @interface RemapInTrace {

  }

  static class MyException extends RuntimeException
  {
    public MyException()
    {
      super();
      List<StackTraceElement> elements = new ArrayList<>();
      StackWalker sw = ExtendedWalker;
      sw.forEach(frame -> elements.add(processFrame(frame)));
      setStackTrace(elements.toArray(new StackTraceElement[] {}));

    }


    private StackTraceElement processFrame(StackFrame frame)
    {
      System.out.println(frame.getDeclaringClass());
      StackTraceElement stackTraceElement = frame.toStackTraceElement();

      try {
        Object[] locals = (Object[]) theLocalMethod.invoke(frame);
        if (locals.length > 0 && isAbstract(frame.getDeclaringClass())) {
          Object thisPtr = locals[0];
          System.out.println(thisPtr);

//          String className = stackTraceElement.getClassName();
          String className = locals[0].getClass().getName();

          StackTraceElement stackTraceElement2 = new StackTraceElement(
              squeezeDefault(frame.getDeclaringClass(), stackTraceElement.getClassLoaderName()),
              stackTraceElement.getModuleName(),
              stackTraceElement.getModuleVersion(),
              className,
              stackTraceElement.getMethodName(),
              stackTraceElement.getFileName(),
              stackTraceElement.getLineNumber()
          );
          if(! stackTraceElement.equals(stackTraceElement2)) {
            int as=0;
          }
          System.out.println(stackTraceElement);
          System.out.println(stackTraceElement2);
          return stackTraceElement2;
        }
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }

      return stackTraceElement;
    }

    private String squeezeDefault(Class<?> clazz, String classLoaderName)
    {
      if ("app".equals(classLoaderName)) {
        return null;
      }
      return classLoaderName;
    }

    private boolean isAbstract(Class<?> clazz)
    {
      int modifiers = clazz.getModifiers();
      return Modifier.isAbstract(modifiers) || Modifier.isInterface(modifiers);
    }

    private static final long serialVersionUID = 1L;

    @Override
    public synchronized Throwable fillInStackTrace()
    {
      getStackTrace();
      return super.fillInStackTrace();
    }
  }

  abstract static class AbstactX implements U
  {
    protected void internalAsd()
    {
      // bad stacktrace
      // does jit likes this at all?

      throw new MyException();// DruidException.defensive("safd");
    }

    /**
     * implementation should be a singel call to super.asd
     */
    public abstract void outerAsd() ;
  }

  static class THIS_IS_IT extends AbstactX
  {
    public void outerAsd()
    {
      int asd=1;
      super.internalAsd();
    }
  }

  public static void main(String[] args)
  {

    try {
      new THIS_IS_IT().internalAsd();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    try {
      new THIS_IS_IT().g();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

}
