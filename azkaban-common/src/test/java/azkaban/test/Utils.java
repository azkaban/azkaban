package azkaban.test;

import static azkaban.ServiceProvider.SERVICE_PROVIDER;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;


public class Utils {

  public static void initServiceProvider() {
    final Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
      }
    });
    // Because SERVICE_PROVIDER is a singleton and it is shared among many tests,
    // need to reset the state to avoid assertion failures.
    SERVICE_PROVIDER.unsetInjector();

    SERVICE_PROVIDER.setInjector(injector);
  }
}
