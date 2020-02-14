import { ItemList, KeyList } from 'aws-sdk/clients/dynamodb';
import { promisify } from './utils';
import { batchGet, batchWrite } from './db';

export const batchWritePromise = async <T>(table: string, updates: T[]) => {
  console.log('batchWrite', updates);
  return promisify(batchWrite)({
    RequestItems: {
      [table]: updates,
    },
  });
};

export const batchGetPromise = async (table: string, keys: KeyList) => {
  console.log('batchGet', keys, table);
  return promisify<ItemList>(batchGet)(
    {
      RequestItems: {
        [table]: { Keys: keys },
      },
      ReturnConsumedCapacity: 'NONE',
    },
    table,
    []
  );
};
