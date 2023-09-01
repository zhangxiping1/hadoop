package org.apache.hadoop.http;

import com.google.common.collect.ImmutableMap;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.resource.JerseyResource;
import org.apache.hadoop.http.resource.MyMessage;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;

public class TestMyHttpServer extends HttpServerFunctionalTest {
  static final Logger LOG = LoggerFactory.getLogger(TestHttpServer.class);
  private static HttpServer2 server;
  private static final int MAX_THREADS = 10;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @SuppressWarnings("serial")
  public static class EchoMapServlet extends HttpServlet {
    @SuppressWarnings("unchecked")
    @Override
    public void doGet(HttpServletRequest request,
                      HttpServletResponse response
    ) throws ServletException, IOException {
      PrintWriter out = response.getWriter();
      Map<String, String[]> params = request.getParameterMap();
      SortedSet<String> keys = new TreeSet<String>(params.keySet());
      for(String key: keys) {
        out.print(key);
        out.print(':');
        String[] values = params.get(key);
        if (values.length > 0) {
          out.print(values[0]);
          for(int i=1; i < values.length; ++i) {
            out.print(',');
            out.print(values[i]);
          }
        }
        out.print('\n');
      }
      out.close();
    }
  }

  @SuppressWarnings("serial")
  public static class EchoServlet extends HttpServlet {
    @SuppressWarnings("unchecked")
    @Override
    public void doGet(HttpServletRequest request,
                      HttpServletResponse response
    ) throws ServletException, IOException {
      PrintWriter out = response.getWriter();
      SortedSet<String> sortedKeys = new TreeSet<String>();
      Enumeration<String> keys = request.getParameterNames();
      while(keys.hasMoreElements()) {
        sortedKeys.add(keys.nextElement());
      }
      for(String key: sortedKeys) {
        out.print(key);
        out.print(':');
        out.print(request.getParameter(key));
        out.print('\n');
      }
      out.close();
    }
  }

  @SuppressWarnings("serial")
  public static class HtmlContentServlet extends HttpServlet {
    @Override
    public void doGet(HttpServletRequest request,
                      HttpServletResponse response
    ) throws ServletException, IOException {
      response.setContentType("text/html");
      PrintWriter out = response.getWriter();
      out.print("hello world");
      out.close();
    }
  }




  /** HTTPServer2 是封装了Jetty的Server，提供了一些额外的功能，比如：Jersey，WebAppContext等
   * WebAppContext 是封装了 ServletContextHandler
   * 在学习HTTPServer2 前 ，先学习一下TestJetty的使用
   */

  @Test
  public void StartHttpServer() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(HttpServer2.HTTP_MAX_THREADS_KEY, 10);
    server = createTestServer(conf);
    server.addServlet("echo", "/echo", EchoServlet.class);
    server.addServlet("echomap", "/echomap", EchoMapServlet.class);
    server.addServlet("htmlcontent", "/htmlcontent", HtmlContentServlet.class);
    server.addServlet("longheader", "/longheader", LongHeaderServlet.class);
    server.addJerseyResourcePackage(  // 实际上调用的 serHol.setInitParameter
        JerseyResource.class.getPackage().getName(), "/jersey/*");

    server.start();
    baseUrl = getServerURL(server);
    LOG.info("HTTP server started: "+ baseUrl);
    System.in.read();
  }

    @Test
    public void testJetty() {
      Server server = new Server(8080);

      ServletContextHandler ctx =
          new ServletContextHandler(ServletContextHandler.NO_SESSIONS);

      ctx.setContextPath("/");
      server.setHandler(ctx);

      ServletHolder serHol = ctx.addServlet(ServletContainer.class, "/rest/*");
      serHol.setInitOrder(1);
      serHol.setInitParameter("jersey.config.server.provider.packages",
          MyMessage.class.getPackage().getName());

      ServletHolder holder = new ServletHolder(new WebServlet());
      Map<String, String> params = ImmutableMap. <String, String> builder()
          .put("acceptRanges", "true")
          .put("dirAllowed", "false")
          .put("gzip", "true")
          .put("useFileMappedBuffer", "true")
          .build();
      holder.setInitParameters(params);
      ctx.setWelcomeFiles(new String[] {"index.html"});
      ctx.addServlet(holder, "/");

      try {
        server.start();
        server.join();
      } catch (Exception ex) {
        java.util.logging.Logger.getLogger("testJetty").log(Level.SEVERE, null, ex);
      } finally {

        server.destroy();
      }
    }
}
