import 'mocha';
import { expect } from 'chai';

import { ServerlessCouchbaseConnection } from '..';

const serverlessCouchbase = ServerlessCouchbaseConnection.Instance;

before((done) => {

   
    serverlessCouchbase.config({
    });

    await serverlessCouchbase.connect().then(() => done()).catch(error => done(error));

})
describe('Serverless couchbase', () => {
    it('it should query couchbase', async () => {
        const connnected = await serverlessCouchbase.query();
    })

    it('it should shutdown couchbase', () => {
        
    })
})