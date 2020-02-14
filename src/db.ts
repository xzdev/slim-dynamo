import * as AWS from 'aws-sdk';
import {
  ListTablesOutput,
  DocumentClient,
  UpdateItemInput,
  Key,
  PutItemInputAttributeMap,
  DeleteItemInput,
  BatchGetItemInput,
  ItemList,
  BatchWriteItemInput,
  KeyList,
} from 'aws-sdk/clients/dynamodb';
import { isEmpty, isArray, forOwn } from 'lodash';
import { request } from 'http';

const dynamodbOfflineOptions = {
  region: 'localhost',
  endpoint: 'http://localhost:8000',
};

const isOffline = () => process.env.IS_OFFLINE;
console.log('Serverless is offline? ', isOffline());

const dynamodb = isOffline()
  ? new AWS.DynamoDB(dynamodbOfflineOptions)
  : new AWS.DynamoDB();

const dbClient = isOffline()
  ? new AWS.DynamoDB.DocumentClient(dynamodbOfflineOptions)
  : new AWS.DynamoDB.DocumentClient();

const dbPrefix = isOffline() ? 'dev' : process.env.DB_PREFIX;

export type Callback<T> = (error: Error | null, value?: T | T[]) => void;

export type CallbackWithKey<T> = (
  error: Error | null,
  callback?: T[],
  key?: Key
) => void;

export type QueryInput = Partial<DocumentClient.QueryInput>;

export const dbListTables = (callback: Callback<ListTablesOutput>) => {
  dynamodb.listTables(
    {
      Limit: 10,
    },
    (error: Error, data: ListTablesOutput) => {
      console.log('List tables', error, data);
      callback(error, data);
    }
  );
};

export const dbGetResult = <T>(
  key: DocumentClient.Key,
  tableName: string,
  callback: Callback<T>
) => {
  dbClient.get(
    {
      Key: key,
      TableName: `${dbPrefix}-${tableName}`,
    },
    (error, result) => {
      console.log(`Get ${tableName} result`, error, result);
      if (error) {
        console.error(`Cannot find ${tableName}`, error);
        callback(error);
      } else {
        console.log(`Return item matches to the ${key}`, result.Item);
        callback(null, result.Item as T);
      }
    }
  );
};

export const dbQueryResultGSI = <T>(
  indexName: string,
  expression: QueryInput,
  tableName: string,
  callback: CallbackWithKey<T>
) => {
  console.log('dbQueryResultGSI', expression, tableName);
  dbClient.query(
    {
      IndexName: indexName,
      TableName: `${dbPrefix}-${tableName}`,
      ...expression,
    },
    (error, getResult) => {
      console.log(`Get ${tableName} result`, error, getResult);
      if (error) {
        console.error(`Cannot find ${tableName}`, error);
        callback(error);
      } else {
        console.log(`Return item matches to the ${indexName}`, getResult.Items);
        callback(null, getResult.Items as T[], getResult.LastEvaluatedKey);
      }
    }
  );
};

// callback of dbScanResult has a different signature than common ones, the third
// optional parameter is LastEvaluatedKey of the scanning result
export const dbScanResult = <T>(
  tableName: string,
  expression = {},
  callback: CallbackWithKey<T>
) => {
  dbClient.scan(
    {
      TableName: `${dbPrefix}-${tableName}`,
      ...expression,
    },
    (error, getResult) => {
      console.log(
        `Get all items inside ${tableName}, total:`,
        error,
        getResult && getResult.Count
      );
      if (error) {
        console.error(`Read ${tableName} error`, error);
        callback(error);
      } else {
        console.log(`Return all items of the ${tableName}`);
        callback(error, getResult.Items as T[], getResult.LastEvaluatedKey);
      }
    }
  );
};

export const dbQueryResult = <T>(
  query: QueryInput,
  tableName: string,
  callback: Callback<T>
) => {
  dbClient.query(
    {
      ...query,
      TableName: `${dbPrefix}-${tableName}`,
    },
    (error, getResult) => {
      if (error) {
        console.error(`Cannot find ${tableName}`, error);
        callback(error);
      } else {
        console.log(`Find items matches to the ${query}`, getResult.Items);
        callback(null, getResult.Items as T[]);
      }
    }
  );
};

export const dbPutResult = (
  input: PutItemInputAttributeMap,
  tableName: string,
  callback: Callback<any>
) => {
  dbClient.put(
    {
      TableName: `${dbPrefix}-${tableName}`,
      Item: input,
    },
    (error, putResult) => {
      console.log('Add result', error, putResult);
      callback(error);
    }
  );
};

export const dbUpdateResult = (
  key: Key,
  update: UpdateItemInput,
  tableName: string,
  callback: Callback<boolean>
) => {
  dbClient.update(
    {
      TableName: `${dbPrefix}-${tableName}`,
      Key: key,
      ...update,
      ReturnValues: 'UPDATED_NEW',
    },
    (error, updateResult) => {
      console.log('Update ', key, error, updateResult);
      callback(error, true);
    }
  );
};

const dbDeleteResult = (
  key: Key,
  condition: DeleteItemInput,
  tableName: string,
  callback: Callback<boolean>
) => {
  dbClient.delete(
    {
      TableName: `${dbPrefix}-${tableName}`,
      Key: key,
      ...condition,
    },
    (error, deleteResult) => {
      console.log('Update ', key, error, deleteResult);
      callback(error, true);
    }
  );
};

const batchWriteRecords = (
  request: DocumentClient.BatchWriteItemInput,
  callback: Callback<boolean>
) => {
  console.log('batchWrite:', request);
  dbClient.batchWrite(request, (error, batchUpdateResult) => {
    if (error) {
      callback(error, false);
      return;
    }

    // TODO: better error handling since there might be ProvisionedThroughputExceededException
    console.log('dbBatchWriteResult:', batchUpdateResult);

    if (
      batchUpdateResult.UnprocessedItems &&
      !isEmpty(batchUpdateResult.UnprocessedItems)
    ) {
      console.warn(
        'Warning, there are unproccessed items',
        batchUpdateResult.UnprocessedItems
      );
      // recursively call the method until callback is called.
      batchWriteRecords(
        { RequestItems: batchUpdateResult.UnprocessedItems },
        callback
      );
    } else {
      // no missing keys, then call the callback to end the recursion
      callback(error, true);
    }
  });
};

export const batchWrite = (
  request: DocumentClient.WriteRequests,
  tableName: string,
  callback: Callback<boolean>
) => {
  console.log('batchWrite', request);
  const table = `${dbPrefix}-${tableName}`;
  batchWriteRecords(
    {
      RequestItems: {
        [table]: request,
      },
    },
    callback
  );
};

const batchGetRecords = (
  request: BatchGetItemInput,
  tableName: string,
  results: ItemList,
  callback: Callback<ItemList>
) => {
  console.log('batchGetRecords:', request);
  dbClient.batchGet(request, (error, batchGetResult) => {
    // TODO: better error handling since there might be ProvisionedThroughputExceededException
    console.log('dbBatchGetResult:', error, batchGetResult);
    if (batchGetResult && batchGetResult.Responses) {
      const response = batchGetResult.Responses[tableName];
      results.push(...response);
    }

    if (
      batchGetResult.UnprocessedKeys &&
      !isEmpty(batchGetResult.UnprocessedKeys)
    ) {
      console.warn(
        'Warning, there are unproccessed keys',
        batchGetResult.UnprocessedKeys
      );
      // recursively call the method until callback is called.
      batchGetRecords(
        { RequestItems: batchGetResult.UnprocessedKeys },
        tableName,
        results,
        callback
      );
    } else {
      // no missing keys, then call the callback to end the recursion
      callback(error, results);
    }
  });
};

export const batchGet = (
  keys: DocumentClient.KeyList,
  tableName: string,
  callback: Callback<ItemList>
) => {
  const table = `${dbPrefix}-${tableName}`;
  console.log('batchGetWithCallback', keys, table);
  batchGetRecords(
    {
      RequestItems: {
        [table]: { Keys: keys },
      },
      ReturnConsumedCapacity: 'NONE',
    },
    table,
    [],
    callback
  );
};

interface DBObject {
  [k: string]: any;
}

type DBValue = DBObject | DBObject[];

const remapItem = (item: DBObject, originalId: string, newId: string) => ({
  ...item,
  [newId]: item[originalId],
});

const remap = (value: DBValue, originalId: string, newId: string) => {
  return isArray(value)
    ? value.map(item => remapItem(item, originalId, newId))
    : remapItem(value, originalId, newId);
};

const swapOutputId = (
  callback: Callback<DBValue>,
  originalId: string,
  newId: string
) => (error: Error, value: DBValue) =>
  callback(error, isEmpty(value) ? value : remap(value, originalId, newId));

const RESERVED_WORDS: { [key: string]: boolean } = {
  name: true,
  status: true,
  location: true,
  catalog: true,
};
const reservedWords = (prop: string) => {
  return RESERVED_WORDS[prop];
};

const generateUpdate = (input: {
  [key: string]: any;
}): Partial<UpdateItemInput> => {
  const names: string[] = [];
  const alias: { [key: string]: any } = {};
  const values: { [key: string]: any } = {};

  forOwn(input, (value, key) => {
    if (reservedWords(key)) {
      names.push(`#${key}=:${key}`);
      alias[`#${key}`] = key;
      values[`:${key}`] = value;
    } else {
      names.push(`${key}=:${key}`);
      values[`:${key}`] = value;
    }
  });

  const express = `set ${names.join(',')}`;
  const attributeNames = alias;
  const attributeValues = values;

  const expressionJSON: Partial<UpdateItemInput> = {
    UpdateExpression: express,
    ExpressionAttributeValues: attributeValues,
  };

  if (!isEmpty(alias)) {
    expressionJSON.ExpressionAttributeNames = attributeNames;
  }

  return expressionJSON;
};
