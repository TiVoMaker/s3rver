'use strict';

const stream = require('stream');
const FileStore = require("./file-store");
const moment = require('moment');
const url = require('url');

const maxLines = 100;

class S3Logger {

  constructor(rootDirectory, appLogger, logBucket, logPrefix, logInterval, bucketsToLog) {
    this.logLines = [];
    this.logPrefix = logPrefix;
    this.logInterval = logInterval;
    this.appLogger = appLogger;
    this.logBucket = logBucket;
    this.bucketsToLog = bucketsToLog;
    if (!Array.isArray(bucketsToLog)) {
      this.bucketsToLog = [bucketsToLog];
    }
    this.fs = new FileStore(rootDirectory);
    this.initialized = false;

    this.appLogger.info('S3 Access Logging Enabled');
    this.appLogger.info(`Log bucket: ${this.logBucket}, Prefix: ${this.logPrefix}, MaxDelay: ${this.logInterval}`);
    this.appLogger.info(`Logging these buckets: ${this.bucketsToLog}`);
  }

  initialize(callback) {
    if (this.initialized) {
      if (callback) callback();
      return;
    }

    this.fs.getBucket(this.logBucket, (err, bucket) => {
      if (err) {
        this.fs.putBucket(this.logBucket, (err, newBucket) => {
          if (err) {
            this.appLogger.warn(`unable to create S3 log bucket ${this.logBucket}`);
            callback(new Error(`unable to create S3 log bucket ${this.logBucket}`));
            return;
          }
          this.bucket = newBucket;
          this.nextCheck();
          this.initialized = true;
          if (callback) callback();
        });
        return;
      }
      this.bucket = bucket;
      this.nextCheck();
      this.initialized = true;
      if (callback) callback();
    });
  }

  nextCheck() {
    if (this.logInterval) {
      setTimeout(this.dumpLogs.bind(this), this.logInterval);
    }
  }

  dumpLogs() {
    if (this.logLines.length > 0) {
      const toLog = this.logLines;
      this.logLines = [];
      this.initialize(() => {
        let buffer = toLog.join('\n');
        let pos = 0;
        let rs = new stream.Readable({
          read(size) {
            while (pos < buffer.length) {
              let left = buffer.length - pos;
              let tosend = (left > size ? size : left);
              if (!this.push(buffer.slice(pos, tosend))) {
                pos += tosend;
                return;
              }
              pos += tosend;
            }
            this.push(null);
          }
        });
        rs.headers = {
          'content-type': 'text/plain'
        };

        this.fs.putObject(
          this.bucket, `${this.logPrefix}${moment().format('YYYY-MM-DD-HH-mm-ss-x')}`,
          rs,
          (err) => {
            if (err) {
              this.appLogger.warn(`unable to write S3 log: ${err.message}`);
              return;
            }
            this.nextCheck();
          }
        );
      });
    } else {
      this.nextCheck();
    }
  }

  logEntry(m) {
    let urlParts = url.parse(m.url);
    let bucket = urlParts.pathname.split('/')[1];

    if (!this.bucketsToLog.includes(bucket)) {
      // not a bucket we're interested in
      return;
    }

    let key = urlParts.pathname.slice(bucket.length+2);
    if (!key) {
      // bucket creation/deletion not interesting, AWS doesn't log it
      return;
    }

    let query = '';
    if (urlParts.search) {
      query = urlParts.search;
    }

    const source = m.headers['x-amz-copy-source'];
    if (m.status === "200" && source) {
      let srcBucket, srcKey;
      let srcParts = decodeURIComponent(source).split('/');
      if (srcParts[0] === '') {
        srcBucket = srcParts[1];
        srcKey = srcParts.slice(2).join('/');
      } else {
        srcBucket = srcParts[0];
        srcKey = srcParts.slice(1).join('/');
      }
      this.logLines.push(
        `- ${srcBucket} [${m.date}] ${m.remoteAddr} - - REST.COPY.OBJECT ` +
        `${srcKey || '-'} "GET /${srcBucket}/${srcKey}${query} HTTP/${m.httpVersion}" ` +
        `${m.status} - ${m.length || '-'} - - "${m.referrer || '-'}" "${m.userAgent || '-'}"`
      );
    }

    this.logLines.push(
      `- ${bucket} [${m.date}] ${m.remoteAddr} - - REST.${m.method}.OBJECT ` +
      `${key || '-'} "${m.method} /${bucket}/${key}${query} HTTP/${m.httpVersion}" ` +
      `${m.status} - ${m.length || '-'} - - "${m.referrer || '-'}" "${m.userAgent || '-'}"`
    );

    if (this.logLines.length >= maxLines) {
      this.dumpLogs();
    }
  }
}

module.exports = S3Logger;
