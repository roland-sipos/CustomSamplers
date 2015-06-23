package hypertable;

import org.hypertable.thrift.ThriftClient;

public class ThriftClientObject {

public class ThriftClientObj {
        public ThriftClient t_ThriftClient = null;
        public long ns_id = -1L;
        public long l_mutator_id = -1L;
        public ThriftClientObj(String m_host, int m_port) throws Exception {
            t_ThriftClient = ThriftClient.create(m_host, m_port);
            ns_id = t_ThriftClient.open_namespace("a");//DB_NAMESPACE);
            l_mutator_id = t_ThriftClient.open_mutator(ns_id, "??" /*DB_TABLE*/, 0, 0);
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

}
