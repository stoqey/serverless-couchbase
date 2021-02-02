import {QueryResult, SofaConnection} from '@stoqey/sofa';
import ServerlessCouchbase, {ConfigArgs} from './serverless.couchbase';
export * from './serverless.couchbase';

SofaConnection.Instance;
ServerlessCouchbase.Instance;

export const query = (query: string, options?: any): Promise<QueryResult> =>
    SofaConnection.Instance.cluster.query(query, options);

export const startCouchbase = async (couchbaseConfig: ConfigArgs): Promise<void> => {
    ServerlessCouchbase.Instance.config(couchbaseConfig);
    return await ServerlessCouchbase.Instance.connect();
};

export default ServerlessCouchbase;
