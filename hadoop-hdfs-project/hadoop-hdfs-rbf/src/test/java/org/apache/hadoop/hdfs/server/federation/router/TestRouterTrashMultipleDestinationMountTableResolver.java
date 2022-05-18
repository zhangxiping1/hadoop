package org.apache.hadoop.hdfs.server.federation.router;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.security.UserGroupInformation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This is a test through the Router move data to the Trash with
 * MultipleDestinationMountTableResolver.
 */
public class TestRouterTrashMultipleDestinationMountTableResolver {

  private static StateStoreDFSCluster cluster;
  private static MiniRouterDFSCluster.RouterContext routerContext;
  private static MountTableResolver resolver;
  private static MiniRouterDFSCluster.NamenodeContext nnContextNs0;
  private static MiniRouterDFSCluster.NamenodeContext nnContextNs1;
  private static FileSystem nnFsNs0;
  private static FileSystem nnFsNs1;

  private static String ns0;
  private static String ns1;
  private static final String TEST_USER = "test-trash";
  private static final String MOUNT_POINT = "/home/data";
  private static final String MOUNT_POINT_CHILD_DIR = MOUNT_POINT + "/test";
  private static final String FILE_NS0 = MOUNT_POINT_CHILD_DIR + "/fileNs0";
  private static final String FILE_NS1 = MOUNT_POINT_CHILD_DIR + "/fileNs1";
  private static final String TRASH_ROOT = "/user/" + TEST_USER + "/.Trash";
  private static final String CURRENT = "/Current";

  @BeforeClass
  public static void globalSetUp() throws Exception {
    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(false, 2,
        MultipleDestinationMountTableResolver.class);
    Configuration routerConf =
        new RouterConfigBuilder().stateStore().admin().quota().rpc().build();

    Configuration hdfsConf = new Configuration(false);
    hdfsConf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    hdfsConf.setInt("fs.trash.interval", 24*60);
    hdfsConf.setInt("fs.trash.checkpoint.interval", 24*60);
    cluster.addRouterOverrides(routerConf);
    cluster.addNamenodeOverrides(hdfsConf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();

    ns0 = cluster.getNameservices().get(0);
    ns1 = cluster.getNameservices().get(1);

    nnContextNs0 = cluster.getNamenode(ns0, null);
    nnFsNs0 = nnContextNs0.getFileSystem();
    nnContextNs1 = cluster.getNamenode(ns1, null);
    nnFsNs1 = nnContextNs1.getFileSystem();

    routerContext = cluster.getRandomRouter();
    resolver =
        (MultipleDestinationMountTableResolver) routerContext.getRouter().getSubclusterResolver();
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.stopRouter(routerContext);
      cluster.shutdown();
      cluster = null;
    }
  }

  private boolean addMountTable(final MountTable entry) throws IOException {
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    AddMountTableEntryRequest addRequest =
        AddMountTableEntryRequest.newInstance(entry);
    AddMountTableEntryResponse addResponse =
        mountTableManager.addMountTableEntry(addRequest);
    // Reload the Router cache
    resolver.loadCache(true);
    return addResponse.getStatus();
  }

  @Test
  public void testMoveToTrashWithMultipleDestinationMountTableResolver() throws IOException,
      URISyntaxException, InterruptedException {

    // add MountPoint  /home/data  ns0 -> /home/data, ns1 -> /home/data
    Map<String, String> destMap = new HashMap<>();
    destMap.put(ns0, MOUNT_POINT);
    destMap.put(ns1, MOUNT_POINT);
    MountTable addEntry = MountTable.newInstance(MOUNT_POINT, destMap);
    addEntry.setDestOrder(DestinationOrder.HASH_ALL);
    assertTrue(addMountTable(addEntry));

    // current user client ,supper user setup permission for testUser
    DFSClient clientNs0 = nnContextNs0.getClient();
    DFSClient clientNs1 = nnContextNs1.getClient();

    clientNs0.setOwner("/", TEST_USER, TEST_USER);
    clientNs1.setOwner("/", TEST_USER, TEST_USER);

    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(TEST_USER);

    // testUser client clientNs0,clientNs1 ,mkdir ns0,ns1 /home/data/test
    clientNs0 = nnContextNs0.getClient(ugi);
    clientNs0.mkdirs(MOUNT_POINT_CHILD_DIR, new FsPermission("777"), true);
    assertTrue(clientNs0.exists(MOUNT_POINT_CHILD_DIR));

    clientNs1 = nnContextNs1.getClient(ugi);
    clientNs1.mkdirs(MOUNT_POINT, new FsPermission("777"), true);
    assertTrue(clientNs1.exists(MOUNT_POINT));

    // create test file for ns0,ns1
    clientNs0.create(FILE_NS0, true);
    clientNs1.create(FILE_NS1, true);

    //The client routerFs accessing the Router
    Configuration routerConf = routerContext.getConf();
    FileSystem routerFs = DFSTestUtil.getFileSystemAs(ugi, routerConf);

    FileStatus[] fileStatuses = routerFs.listStatus(new Path(MOUNT_POINT_CHILD_DIR));
    assertEquals(2, fileStatuses.length);
    assertEquals(TEST_USER, fileStatuses[0].getOwner());

    // Ask the router to move the files under the mount point to Trash
    Trash trash = new Trash(routerFs, routerConf);
    assertTrue(trash.moveToTrash(new Path(MOUNT_POINT_CHILD_DIR)));

    // Although the user's trash path is not mounted,
    // the files of both SubDfsCluster can be moved to Trash successfully
    fileStatuses = nnFsNs0.listStatus(
        new Path(TRASH_ROOT + CURRENT + MOUNT_POINT_CHILD_DIR));
    assertEquals(1, fileStatuses.length);

    fileStatuses = nnFsNs1.listStatus(
        new Path(TRASH_ROOT + CURRENT + MOUNT_POINT_CHILD_DIR));
    assertEquals(1, fileStatuses.length);

    assertTrue(nnFsNs0.exists(new Path(TRASH_ROOT + CURRENT + FILE_NS0)));
    assertTrue(nnFsNs1.exists(new Path(TRASH_ROOT + CURRENT + FILE_NS1)));

    // The Trash path file is also accessible through the router,
    // although the user's Trash is not mounted
    fileStatuses = routerFs.listStatus(
        new Path(TRASH_ROOT + CURRENT + MOUNT_POINT_CHILD_DIR));
    assertEquals(2, fileStatuses.length);
  }

}
