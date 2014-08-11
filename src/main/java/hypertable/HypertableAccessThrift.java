package hypertable;

import java.util.ArrayList;
import java.util.Vector;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.Key;

public class HypertableAccessThrift {
    /*public static AtomicLong l_time = new AtomicLong();
    public static AtomicLong l_total_req = new AtomicLong();
    public static final String DB_NAMESPACE = "test";
    public static final String DB_TABLE = "cache";
    public static final String DB_TABLE_COL_KEY = "RecId";
    public static final String DB_TABLE_COL_VALUE = "Record";
    public static final String DB_TABLE_CREATE_Q = "CREATE TABLE " + DB_TABLE + " ( " + DB_TABLE_COL_KEY + ", " + DB_TABLE_COL_VALUE + "); ";
    public Executors _insert_executors[] = null;
    public Executors _read_executors[] = null;
    ThriftClientObj t_ThriftClientObj[] = null;

    public HypertableAccessThrift(String a_host, int a_port, int a_th_count) {
        t_ThriftClientObj = new ThriftClientObj[a_th_count];
        for (int i = 0; i < t_ThriftClientObj.length; i++) {
            try {
                t_ThriftClientObj[i] = new ThriftClientObj(a_host, a_port);
            } catch (Exception ex) {
                Logger.getLogger(HypertableAccessThrift.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        _insert_executors = new Executors[a_th_count];
        for (int j = 0; j < _insert_executors.length; j++) {
            _insert_executors[j] = new Executors();
        }
    }
    public static void main(String str[]) {
        HypertableAccessThrift h_AccessThrift = new HypertableAccessThrift(str[0], Integer.valueOf(str[1]), Integer.valueOf(str[2]));
        h_AccessThrift.process(Integer.valueOf(str[2]), Integer.valueOf(str[3]));
    }
    private void process(int th_count, int l_count) {
        long l_start_time = System.currentTimeMillis();
        for (int i = 0; i < l_count; i++) {
            _insert_executors[i % th_count].runTask(new InsertionHypertable("" + VeGenUniqueId.decodeLong(VeGenUniqueId.veGenUniqId(1, 1)), "", t_ThriftClientObj[i % th_count]));
        }
        while (true) {
            if (l_total_req.get() >= l_count) {
                for (int i = 0; i < t_ThriftClientObj.length; i++) {
                    try {
                        t_ThriftClientObj[i].close();
                    } catch (Exception ex) {
                        Logger.getLogger(HypertableAccessThrift.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                System.out.println("Total Time : " + (System.currentTimeMillis() - l_start_time));
                System.out.println("Total Requests : " + l_total_req.get());
                System.exit(1);
                break;
            }
        }
    }
    class ThriftClientObj {
        public ThriftClient t_ThriftClient = null;
        public long ns_id = -1L;
        public long l_mutator_id = -1L;
        public ThriftClientObj(String m_host, int m_port) throws Exception {
            t_ThriftClient = ThriftClient.create(m_host, m_port);
            ns_id = t_ThriftClient.open_namespace(DB_NAMESPACE);
            l_mutator_id = t_ThriftClient.open_mutator(ns_id, DB_TABLE, 0, 0);
            if (l_mutator_id == -1) {
                System.out.println(l_mutator_id);
            }
        }
        public void close() throws Exception {
            t_ThriftClient.flush_mutator(l_mutator_id);
            t_ThriftClient.close_mutator(l_mutator_id);
            t_ThriftClient.close_namespace(ns_id);
            t_ThriftClient.close();
        }
    }

    class InsertionHypertable implements Runnable {
        private final String m_key;
        private final String m_value;
        private final ThriftClientObj m_object;
        public InsertionHypertable(String a_key, String a_value, ThriftClientObj a_object) {
            m_key = a_key;
            m_value = a_value;
            m_object = a_object;
        }
        public void run() {
            try {
                ArrayList<Cell> l_arr_list = new ArrayList<Cell>();
                for (int i = 0; i < 10; i++) {
                    Cell l_cell = new Cell();
                    Key l_key = new Key();
                    l_key.setColumn_family(DB_TABLE_COL_KEY);
                    l_key.setRow(m_key);
                    l_cell.setKey(l_key);
                    l_cell.setValue("THRIFTCLIENT".getBytes());
                    l_arr_list.add(l_cell);
                }
                m_object.t_ThriftClient.set_cells(m_object.l_mutator_id, l_arr_list);
                m_object.t_ThriftClient.flush_mutator(m_object.l_mutator_id);
            } catch (Exception ex) {
                Logger.getLogger(HypertableAccessThrift.class.getName()).log(Level.SEVERE, null, ex);
            }
            synchronized (HypertableAccessThrift.l_total_req) {
                HypertableAccessThrift.l_total_req.set(HypertableAccessThrift.l_total_req.get() + 1);
            }
        }
    }*/
}
