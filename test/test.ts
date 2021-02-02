import 'mocha';
import {expect} from 'chai';

import {startCouchbase, query} from '../src/index';

before((done) => {
    const config = {
        connectionString: 'couchbase://localhost',
        bucketName: 'stq',
        username: 'admin',
        password: '123456',
    };
    startCouchbase(config)
        .then(() => done())
        .catch((error) => done(error));
        
});
describe('Serverless couchbase', () => {
    it('it should query couchbase', async () => {
        const queryString = `SELECT * FROM \`stq\` WHERE _type="User"`;
        const queryResults = await query(queryString);
        console.log('query results', queryResults);
        expect(queryResults.rows).to.be.not.null;
    });
});
