package example;

import com.ceph.rados.IoCTX;
import com.ceph.rados.Rados;
import com.ceph.rados.exceptions.RadosException;

import java.io.File;

public class RadosClient {
    private static RadosClient rc = null;
    private static Rados rados = null;
    private static IoCTX ioctx;
    private static boolean poolCreated = false;

    public static RadosClient getInstance() {
        if (rc == null)
            rc = new RadosClient();
        return rc;
    }

    public void createPool(String poolName) {
        if (poolCreated == false) {
            try {
                ioctx = rados.ioCtxCreate(poolName);
                String [] allOids = ioctx.listObjects();
                for (int i = 0; i < allOids.length; i++) {
                    ioctx.remove(allOids[i]);
                }
                poolCreated = true;
            } catch (RadosException e) {
                e.printStackTrace();
            }
        }
    }

    public void writeObject(String objectName, String data) {

    }

    public void destroyPool(String poolName) {
        if (poolCreated == true) {
            //TODO
        }
    }


    private boolean runQuery(String command, String output) {
        return false;
    }

    private RadosClient() {
        rados = new Rados("admin");
        File f = new File("/etc/ceph/ceph.conf");
        try {
            rados.confReadFile(f);
            rados.connect();
        } catch (RadosException e) {
            System.out.println(e.getMessage() + ": " + e.getReturnValue());
        }

    }

}
