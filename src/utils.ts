type Callback = (...args: any[]) => void;

export const promisify = <T>(func: Callback) => {
  return (...args: any[]) =>
    new Promise((resolve, reject) => {
      func(...args, (error: Error, value: T) => {
        if (error) {
          reject(error);
        } else {
          resolve(value);
        }
      });
    });
};

interface AnyValueObject {
  [key: string]: any;
}

export const assignTruthyProps = (
  target: AnyValueObject,
  source: AnyValueObject
) => {
  Object.keys(source).forEach(prop => {
    // truthy props, then assign it to target
    if (source[prop] !== undefined || source[prop] !== null) {
      target[prop] = source[prop];
    }
  });
  return target;
};
